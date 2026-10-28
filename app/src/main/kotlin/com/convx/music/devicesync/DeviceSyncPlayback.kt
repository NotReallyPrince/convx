/**
 * Convx Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 *
 * Plays the part of one device in the account's playback session: the output that the
 * sound comes from, or a remote that steers whichever device is.
 */

package com.convx.music.devicesync

import android.os.SystemClock
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import androidx.media3.common.Timeline
import com.convx.music.extensions.metadata
import com.convx.music.extensions.toMediaItem
import com.convx.music.jam.JamManager
import com.convx.music.listentogether.ListenTogetherManager
import com.convx.music.models.MediaMetadata
import com.convx.music.playback.MusicService
import com.convx.music.playback.queues.ListQueue
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import timber.log.Timber
import javax.inject.Inject
import javax.inject.Singleton

/** A song as the web player describes it; the id is a YouTube video id on both sides. */
@Serializable
data class SyncTrack(
    val id: String,
    val title: String,
    val artist: String = "",
    val artistId: String? = null,
    val album: String? = null,
    val durationLabel: String = "",
    val durationSeconds: Double = 0.0,
    val thumbnail: String? = null,
)

/** One `client-state`: what the output is playing, stamped with the server's clock. */
@Serializable
data class SyncState(
    val device: String,
    val output: String,
    val seq: Int = 0,
    val isPlaying: Boolean = false,
    val track: SyncTrack? = null,
    /** Left out when unchanged since the sender's last message; [queueHash] still describes it. */
    val queue: List<SyncTrack>? = null,
    val queueHash: String = "",
    val index: Int = 0,
    /** Seconds. */
    val position: Double = 0.0,
    /** Server milliseconds at which [position] was read. */
    val at: Long = 0L,
    val contextLabel: String? = null,
    val shuffle: Boolean = false,
    val repeat: String = "off",
)

@Singleton
class DeviceSyncPlayback @Inject constructor(
    private val client: DeviceSyncClient,
    private val listenTogether: dagger.Lazy<ListenTogetherManager>,
    private val jam: dagger.Lazy<JamManager>,
) {
    /** The output's playback, as last heard, for showing and steering it from here. */
    data class Remote(
        val deviceId: String,
        val deviceName: String,
        val track: SyncTrack?,
        val queue: List<SyncTrack>,
        val index: Int,
        val isPlaying: Boolean,
        val position: Double,
        val at: Long,
    ) {
        fun positionAt(serverNow: Long): Double {
            val elapsed = if (isPlaying) (serverNow - at).coerceAtLeast(0L) / 1000.0 else 0.0
            val duration = track?.durationSeconds ?: 0.0
            val now = position + elapsed
            return if (duration > 0) now.coerceAtMost(duration) else now
        }
    }

    data class UiState(
        val live: Boolean = false,
        /** The account's other devices. */
        val devices: List<DeviceSyncClient.Device> = emptyList(),
        val outputId: String? = null,
        /** Set while another device is the output. */
        val remote: Remote? = null,
    )

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val json = Json { ignoreUnknownKeys = true; explicitNulls = false; encodeDefaults = true }
    private val self get() = client.deviceId

    private val _ui = MutableStateFlow(UiState())
    val ui: StateFlow<UiState> = _ui.asStateFlow()

    // All state below is confined to the main thread, where the player lives.
    private var outputId: String? = null
    private var claimSeq = 0
    private var hasAnnounced = false
    private var remote: Remote? = null
    private var sentQueue: String? = null
    private var echoUntil = 0L
    private var outputMissingSince = 0L
    private var commandSeq = 0
    private val pending = mutableMapOf<String, Pair<String, Job>>()
    private val applied = LinkedHashSet<String>()

    // The service, not the app's screen: commands must still land with the screen off.
    private var service: MusicService? = null
    private var player: Player? = null
    private var playerJob: Job? = null
    private var started = false

    private val isOutput get() = outputId == null || outputId == self

    private val live: Boolean
        get() = client.state.value.status == DeviceSyncClient.Status.Connected && !inSession()


    private val listener = object : Player.Listener {
        override fun onPlayWhenReadyChanged(playWhenReady: Boolean, reason: Int) {
            if (!live || echoing()) return
            if (playWhenReady) {
                // Pressing play here takes the music back, whichever device had it.
                if (outputId != self) claimOutput(self)
                broadcast()
            } else if (isOutput && hasAnnounced) {
                broadcast()
            }
        }

        override fun onMediaItemTransition(mediaItem: MediaItem?, reason: Int) = announceChange()

        override fun onPositionDiscontinuity(oldPosition: Player.PositionInfo, newPosition: Player.PositionInfo, reason: Int) {
            if (reason == Player.DISCONTINUITY_REASON_SEEK) announceChange()
        }

        override fun onTimelineChanged(timeline: Timeline, reason: Int) {
            if (reason == Player.TIMELINE_CHANGE_REASON_PLAYLIST_CHANGED) announceChange(withQueue = true)
        }

        override fun onShuffleModeEnabledChanged(shuffleModeEnabled: Boolean) = announceChange()

        override fun onRepeatModeChanged(repeatMode: Int) = announceChange()
    }

    fun start() {
        if (started) return
        started = true
        scope.launch { client.events.collect(::onEvent) }
        scope.launch { client.state.collect { onSessionChanged() } }
        scope.launch {
            while (true) {
                delay(HEARTBEAT_MS)
                tick()
            }
        }
    }

    /* ── Steering from this device ──────────────────────────────────────── */

    fun togglePlayPause() {
        val remote = remote ?: return
        sendCommand(buildJsonObject { put("kind", if (remote.isPlaying) "pause" else "play") })
    }

    fun skip(delta: Int) {
        val remote = remote ?: return
        val index = remote.index + delta
        if (index !in remote.queue.indices) return
        sendCommand(loadCommand(remote.queue, index, position = 0.0))
    }

    fun seekTo(seconds: Double) {
        sendCommand(buildJsonObject {
            put("kind", "seek")
            put("position", seconds)
        })
    }

    /** Moves the music to [target], carrying on from where it is. */
    fun transferTo(target: String) {
        if (!live) return
        if (target == self) return playHere()
        val (queue, index, position, playing) = currentPlayback() ?: return
        claimOutput(target)
        val lead = if (playing) HANDOVER_LEAD_S else 0.0
        val id = nextCommandId()
        trackCommand(target, id, loadCommand(queue, index, position + lead))
        remote = Remote(target, nameOf(target), queue.getOrNull(index), queue, index, playing, position + lead, client.serverNow())
        if (player?.playWhenReady == true) pauseQuietly()
        publishUi()
    }

    /** Takes the music from the output and plays it on this phone from the same spot. */
    fun playHere() {
        val remote = remote ?: return
        val queue = remote.queue.ifEmpty { listOfNotNull(remote.track) }
        if (queue.isEmpty()) return
        claimOutput(self)
        load(queue, remote.index.coerceIn(queue.indices), remote.positionAt(client.serverNow()))
        scope.launch {
            delay(START_SETTLE_MS)
            broadcast()
        }
    }

    /* ── Incoming ───────────────────────────────────────────────────────── */

    private fun onEvent(event: DeviceSyncClient.Event) {
        if (!live) return
        val data = event.data
        when (event.name) {
            "client-state" -> runCatching { json.decodeFromJsonElement<SyncState>(data) }
                .onSuccess(::onState)
                .onFailure { Timber.tag(TAG).w(it, "Unreadable state") }

            "client-hello" -> if (isOutput && player?.playWhenReady == true) broadcast(withQueue = true)

            "client-ack" -> if (data.string("to") == self) {
                data.string("id")?.let { pending.remove(it)?.second?.cancel() }
            }

            "client-command" -> if (data.string("to") == self) onCommand(data)
        }
    }

    private fun onState(message: SyncState) {
        // A remote repeating the play state it mirrors from this phone is not a claim. Taking it as
        // one handed the output to a browser that only displays the song, and paused it here quietly.
        if (message.output != message.device && message.output != ALL_DEVICES) return
        if (message.seq < claimSeq) return
        // A device that has not yet acknowledged a handover is still describing the old session.
        if (pending.values.any { it.first == message.device }) return
        val newGeneration = message.seq > claimSeq
        claimSeq = message.seq
        // ponytail: "every device plays" is followed as if the sender alone were the output.
        val claims = newGeneration || message.isPlaying || message.device == outputId || message.output == ALL_DEVICES
        if (!claims) return

        outputId = message.device
        if (player?.playWhenReady == true) pauseQuietly()

        val queue = message.queue ?: remote?.queue.orEmpty()
        if (message.queue == null && message.queueHash.isNotEmpty() && queueHash(queue.map { it.id }) != message.queueHash) {
            client.publish("client-hello", buildJsonObject { put("device", self) })
        }
        remote = Remote(
            deviceId = message.device,
            deviceName = nameOf(message.device),
            track = message.track,
            queue = queue,
            index = message.index,
            isPlaying = message.isPlaying,
            position = message.position,
            at = message.at,
        )
        publishUi()
    }

    private fun onCommand(message: JsonObject) {
        val from = message.string("device") ?: return
        val id = message.string("id") ?: return
        val command = message["command"] as? JsonObject ?: return
        // Unacknowledged, the sender retries and then plays it itself, rather than believing this phone did.
        val player = player ?: return
        client.publish("client-ack", buildJsonObject {
            put("device", self)
            put("to", from)
            put("id", id)
        })
        // A retried command whose ack was lost arrives twice; run it once.
        if (!applied.add(id)) return
        if (applied.size > APPLIED_MEMORY) applied.remove(applied.first())

        val kind = command.string("kind")
        if (kind == "queue") {
            replaceUpcoming(player, tracksOf(command), command.int("index") ?: return)
            scope.launch {
                delay(QUEUE_SETTLE_MS)
                broadcast(withQueue = true)
            }
            return
        }

        claimOutput(self)
        when (kind) {
            "play" -> play(player)
            "pause" -> player.playWhenReady = false
            "seek" -> command.double("position")?.let { player.seekTo((it * 1000).toLong()) }
            "shuffle" -> command.bool("value")?.let { player.shuffleModeEnabled = it }
            "repeat" -> player.repeatMode = repeatModeOf(command.string("value"))
            "load" -> {
                val queue = tracksOf(command)
                if (queue.isNotEmpty()) {
                    load(queue, (command.int("index") ?: 0).coerceIn(queue.indices), command.double("position") ?: 0.0)
                }
            }
        }
        scope.launch {
            delay(if (kind == "play" || kind == "load") START_SETTLE_MS else COMMAND_SETTLE_MS)
            broadcast()
        }
    }

    /* ── Being the output ───────────────────────────────────────────────── */

    private fun announceChange(withQueue: Boolean = false) {
        if (live && isOutput && hasAnnounced && !echoing()) broadcast(withQueue)
    }

    private fun broadcast(withQueue: Boolean = false) {
        if (!live || !isOutput) return
        val player = player ?: return
        val current = player.currentMediaItem?.metadata ?: return
        // ponytail: a file on this phone means nothing to the other devices, so it is not announced at all.
        if (current.isLocalFile) return

        val items = (0 until player.mediaItemCount).mapNotNull { player.getMediaItemAt(it).metadata }
        val playable = items.filterNot { it.isLocalFile }
        val index = items.take(player.currentMediaItemIndex).count { !it.isLocalFile }
        val queue = playable.map(::toTrack)
        val ids = queue.map { it.id }
        val signature = ids.joinToString(",")
        val sendQueue = withQueue || signature != sentQueue
        sentQueue = signature
        hasAnnounced = true

        val state = SyncState(
            device = self,
            output = outputId ?: self,
            seq = claimSeq,
            isPlaying = player.playWhenReady,
            track = toTrack(current),
            queue = if (sendQueue) queue else null,
            queueHash = queueHash(ids),
            index = index,
            position = player.currentPosition / 1000.0,
            at = client.serverNow(),
            contextLabel = service?.queueTitle,
            shuffle = player.shuffleModeEnabled,
            repeat = when (player.repeatMode) {
                Player.REPEAT_MODE_ALL -> "all"
                Player.REPEAT_MODE_ONE -> "one"
                else -> "off"
            },
        )
        client.publish("client-state", json.encodeToJsonElement(state).jsonObject)
    }

    private fun load(queue: List<SyncTrack>, index: Int, positionSeconds: Double) {
        val service = service ?: return
        val player = player ?: return
        val positionMs = (positionSeconds * 1000).toLong().coerceAtLeast(0L)
        val sameQueue = player.mediaItemCount == queue.size &&
            queue.indices.all { player.getMediaItemAt(it).mediaId == queue[it].id }
        if (sameQueue && player.currentMediaItemIndex == index) {
            player.seekTo(positionMs)
            play(player)
            return
        }
        service.playQueue(ListQueue(items = queue.map(::toMediaItem), startIndex = index, position = positionMs))
    }

    /** Swaps what plays after the current song, leaving the song itself alone. */
    private fun replaceUpcoming(player: Player, queue: List<SyncTrack>, index: Int) {
        if (index !in queue.indices || player.currentMediaItem?.mediaId != queue[index].id) return
        val after = player.currentMediaItemIndex + 1
        if (after < player.mediaItemCount) player.removeMediaItems(after, player.mediaItemCount)
        player.addMediaItems(queue.drop(index + 1).map(::toMediaItem))
    }

    /* ── Session bookkeeping ────────────────────────────────────────────── */

    private fun tick() {
        if (!live) return
        val player = player
        if (isOutput && player?.playWhenReady == true && player.currentMediaItem != null) {
            // Already playing when this device joined: say so, which makes it the output.
            if (outputId != self) claimOutput(self)
            broadcast()
        }
        releaseMissingOutput()
        publishUi()
    }

    /** The output vanished from the device list: after a grace period, stop waiting for it. */
    private fun releaseMissingOutput() {
        val output = outputId
        val devices = client.state.value.devices
        if (output == null || output == self || devices.isEmpty() || devices.any { it.id == output }) {
            outputMissingSince = 0L
            return
        }
        val now = SystemClock.elapsedRealtime()
        if (outputMissingSince == 0L) outputMissingSince = now
        if (now - outputMissingSince >= OUTPUT_GRACE_MS) release()
    }

    private fun onSessionChanged() {
        if (!live) {
            release()
            pending.values.forEach { it.second.cancel() }
            pending.clear()
        }
        publishUi()
    }

    private fun release() {
        outputId = null
        remote = null
        hasAnnounced = false
        outputMissingSince = 0L
        publishUi()
    }

    private fun claimOutput(next: String) {
        // Server-clock seconds, not a count: a counter restarted at 1 whenever the system killed the
        // app, and a browser that had reached 7 ignored every state from this phone as stale.
        // Seconds fit the Int on the wire until 2038; +1 orders two claims within one second.
        claimSeq = maxOf(claimSeq + 1, (client.serverNow() / 1000).toInt())
        outputId = next
        hasAnnounced = true
        if (next == self) remote = null
        publishUi()
    }

    /** Called by [MusicService] as it starts and stops; the player inside it can be swapped while it runs. */
    fun attachService(service: MusicService?) {
        this.service = service
        playerJob?.cancel()
        playerJob = service?.let { scope.launch { it.playerFlow.collect(::attachPlayer) } }
        if (service == null) attachPlayer(null)
    }

    private fun attachPlayer(next: Player?) {
        player?.removeListener(listener)
        player = next
        next?.addListener(listener)
    }

    private fun play(player: Player) {
        when (player.playbackState) {
            Player.STATE_IDLE -> player.prepare()
            Player.STATE_ENDED -> player.seekTo(player.currentMediaItemIndex, 0)
        }
        player.playWhenReady = true
    }

    private fun sendCommand(command: JsonObject) {
        val target = outputId?.takeIf { it != self } ?: return
        trackCommand(target, nextCommandId(), command)
    }

    /** Publishes until acknowledged; an output that never answers is let go. */
    private fun trackCommand(target: String, id: String, command: JsonObject) {
        val frame = buildJsonObject {
            put("device", self)
            put("to", target)
            put("id", id)
            put("command", command)
        }
        pending[id] = target to scope.launch {
            repeat(COMMAND_ATTEMPTS) {
                client.publish("client-command", frame)
                delay(COMMAND_ACK_MS)
            }
            pending.remove(id)
            if (outputId == target) release()
        }
    }

    private fun nextCommandId(): String {
        commandSeq += 1
        return "$self:$commandSeq"
    }

    private fun pauseQuietly() {
        echoUntil = SystemClock.elapsedRealtime() + ECHO_GUARD_MS
        player?.playWhenReady = false
    }

    private fun echoing() = SystemClock.elapsedRealtime() < echoUntil

    private data class Playback(val queue: List<SyncTrack>, val index: Int, val position: Double, val playing: Boolean)

    /** What is playing right now, wherever it is playing. */
    private fun currentPlayback(): Playback? {
        remote?.let { return Playback(it.queue.ifEmpty { listOfNotNull(it.track) }, it.index, it.positionAt(client.serverNow()), it.isPlaying) }
        val player = player ?: return null
        val items = (0 until player.mediaItemCount).mapNotNull { player.getMediaItemAt(it).metadata }
        if (items.isEmpty() || player.currentMediaItem?.metadata?.isLocalFile != false) return null
        val index = items.take(player.currentMediaItemIndex).count { !it.isLocalFile }
        val queue = items.filterNot { it.isLocalFile }.map(::toTrack)
        return Playback(queue, index, player.currentPosition / 1000.0, player.playWhenReady)
    }

    private fun inSession(): Boolean =
        runCatching { listenTogether.get().isInRoom || jam.get().session.value != null }.getOrDefault(false)

    private fun nameOf(deviceId: String) =
        client.state.value.devices.firstOrNull { it.id == deviceId }?.name ?: "Another device"

    private fun publishUi() {
        val devices = client.state.value.devices
        _ui.value = UiState(
            live = live,
            devices = devices.filterNot { it.isSelf },
            outputId = outputId,
            remote = remote?.let { it.copy(deviceName = nameOf(it.deviceId)) }.takeIf { !isOutput },
        )
    }

    private fun loadCommand(queue: List<SyncTrack>, index: Int, position: Double) = buildJsonObject {
        put("kind", "load")
        put("queue", json.encodeToJsonElement(queue))
        put("index", index)
        put("contextLabel", null as String?)
        put("position", position)
    }

    private fun tracksOf(command: JsonObject): List<SyncTrack> =
        runCatching { json.decodeFromJsonElement<List<SyncTrack>>(command["queue"]!!.jsonArray) }.getOrDefault(emptyList())

    private fun JsonObject.string(key: String) = this[key]?.jsonPrimitive?.contentOrNull
    private fun JsonObject.int(key: String) = this[key]?.jsonPrimitive?.intOrNull
    private fun JsonObject.double(key: String) = this[key]?.jsonPrimitive?.doubleOrNull
    private fun JsonObject.bool(key: String) = this[key]?.jsonPrimitive?.booleanOrNull

    companion object {
        private const val TAG = "DeviceSyncPlayback"
        private const val ALL_DEVICES = "all"
        private const val HEARTBEAT_MS = 4_000L
        private const val ECHO_GUARD_MS = 700L
        private const val OUTPUT_GRACE_MS = 90_000L
        private const val COMMAND_ACK_MS = 2_000L
        private const val COMMAND_ATTEMPTS = 3
        private const val START_SETTLE_MS = 1_500L
        private const val COMMAND_SETTLE_MS = 250L
        private const val QUEUE_SETTLE_MS = 100L
        private const val APPLIED_MEMORY = 64
        /** Seconds the receiving device starts ahead, covering the time it takes to start playing. */
        private const val HANDOVER_LEAD_S = 0.9

        /** Must match the web player's hash exactly: FNV-1a over the comma-joined ids. */
        fun queueHash(ids: List<String>): String {
            var hash = 0x811c9dc5.toInt()
            for (char in ids.joinToString(",")) {
                hash = hash xor char.code
                hash *= 0x01000193
            }
            return "${ids.size}:${hash.toUInt().toLong().toString(36)}"
        }

        private fun repeatModeOf(value: String?) = when (value) {
            "all" -> Player.REPEAT_MODE_ALL
            "one" -> Player.REPEAT_MODE_ONE
            else -> Player.REPEAT_MODE_OFF
        }

        private fun toTrack(meta: MediaMetadata): SyncTrack {
            val seconds = meta.duration.coerceAtLeast(0)
            return SyncTrack(
                id = meta.id,
                title = meta.title,
                artist = meta.artists.joinToString { it.name },
                artistId = meta.artists.firstOrNull()?.id,
                album = meta.album?.title,
                durationLabel = "%d:%02d".format(seconds / 60, seconds % 60),
                durationSeconds = seconds.toDouble(),
                thumbnail = meta.thumbnailUrl,
            )
        }

        private fun toMediaItem(track: SyncTrack): MediaItem = MediaMetadata(
            id = track.id,
            title = track.title,
            artists = listOf(MediaMetadata.Artist(id = track.artistId, name = track.artist)),
            duration = track.durationSeconds.toInt().takeIf { it > 0 } ?: -1,
            thumbnailUrl = track.thumbnail,
        ).toMediaItem()
    }
}
