/**
 * Convx Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 *
 * Joins the music web player's per-account playback channel as one more device.
 */

package com.convx.music.devicesync

import android.content.Context
import android.os.Build
import com.convx.music.di.ApplicationScope
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import okhttp3.Cookie
import okhttp3.FormBody
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import timber.log.Timber
import java.io.IOException
import java.util.UUID
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class DeviceSyncClient @Inject constructor(
    @ApplicationContext context: Context,
    @ApplicationScope private val scope: CoroutineScope,
) {
    enum class Status { SignedOut, Connecting, Connected, Disabled, Offline }

    data class Device(val id: String, val name: String, val isMobile: Boolean, val isSelf: Boolean)

    data class State(
        val status: Status = Status.SignedOut,
        val devices: List<Device> = emptyList(),
        /** The newest `client-state` another device published, verbatim. */
        val remotePlayback: JsonObject? = null,
        val message: String? = null,
    )

    /** Raised for a refusal the user can act on; [signedOut] means the session no longer exists. */
    class SyncException(message: String, val signedOut: Boolean = false) : IOException(message)

    private val preferences = context.getSharedPreferences(PREFERENCES, Context.MODE_PRIVATE)
    private val json = Json { ignoreUnknownKeys = true }
    private val http = OkHttpClient.Builder()
        .connectTimeout(15, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .build()

    private val _state = MutableStateFlow(State())
    val state: StateFlow<State> = _state.asStateFlow()

    private var session: Job? = null

    val deviceId: String = preferences.getString(KEY_DEVICE, null)
        ?: UUID.randomUUID().toString().also { preferences.edit().putString(KEY_DEVICE, it).apply() }

    private val token: String? get() = preferences.getString(KEY_TOKEN, null)

    /** Reconnects a device paired in an earlier run. Safe to call repeatedly. */
    fun start() {
        if (token == null || session?.isActive == true) return
        session = scope.launch(Dispatchers.IO) { keepConnected() }
    }

    /**
     * Spends a code from "Link a device" on the web player, or the link it shares
     * (`…#link=<token>`), and adopts that browser's session.
     */
    suspend fun pair(input: String) = withContext(Dispatchers.IO) {
        val trimmed = input.trim()
        val linkToken = Regex("""(?:^|[#&])link=([^&\s]+)""").find(trimmed)?.groupValues?.get(1)
        val body = buildJsonObject {
            if (linkToken != null) put("token", linkToken) else put("code", trimmed.uppercase())
        }
        val request = Request.Builder()
            .url("$BASE_URL/api/music/link/claim")
            .post(body.toString().toRequestBody(JSON_TYPE))
            .build()
        val sessionToken = execute(request) { response ->
            if (!response.isSuccessful) throw SyncException(errorOf(response) ?: "That code is wrong or has expired.")
            Cookie.parseAll(request.url, response.headers).firstOrNull { it.name == SESSION_COOKIE }?.value
                ?: throw SyncException("The server did not hand back a session.")
        }
        preferences.edit().putString(KEY_TOKEN, sessionToken).commit()
        session?.cancel()
        session = null
        start()
    }

    fun unpair() {
        session?.cancel()
        session = null
        preferences.edit().remove(KEY_TOKEN).apply()
        _state.value = State()
    }

    private suspend fun keepConnected() {
        var backoffMs = MIN_BACKOFF_MS
        while (true) {
            _state.update { it.copy(status = Status.Connecting, message = null) }
            try {
                val config = fetchConfig()
                if (config == null) {
                    delay(DISABLED_RECHECK_MS)
                    continue
                }
                runSocket(config) { backoffMs = MIN_BACKOFF_MS }
            } catch (e: CancellationException) {
                throw e
            } catch (e: SyncException) {
                if (e.signedOut) {
                    preferences.edit().remove(KEY_TOKEN).apply()
                    _state.value = State(message = e.message)
                    return
                }
                _state.update { it.copy(status = Status.Offline, devices = emptyList(), message = e.message) }
            } catch (e: IOException) {
                Timber.tag(TAG).w(e, "Device sync dropped")
                _state.update { it.copy(status = Status.Offline, devices = emptyList(), message = null) }
            }
            // ponytail: plain backoff, no connectivity callback; reconnects within a minute of the network returning.
            delay(backoffMs)
            backoffMs = (backoffMs * 2).coerceAtMost(MAX_BACKOFF_MS)
        }
    }

    private class SocketConfig(val channel: String, val url: String)

    /** Null when the account has sync switched off, or the server has no socket. */
    private fun fetchConfig(): SocketConfig? {
        val request = authorized(Request.Builder().url("$BASE_URL/api/music/sync")).build()
        val body = execute(request) { response ->
            if (!response.isSuccessful) throw SyncException("The music server answered ${response.code}.")
            json.parseToJsonElement(response.body?.string().orEmpty()).jsonObject
        }
        val channel = body.string("channel")
        val socket = body["socket"] as? JsonObject
        if (body["enabled"]?.jsonPrimitive?.booleanOrNull != true || channel == null || socket == null) {
            if (body.string("reason") == "anonymous") throw SyncException("This device was signed out.", signedOut = true)
            _state.update {
                it.copy(
                    status = Status.Disabled,
                    devices = emptyList(),
                    message = if (body.string("reason") == "disabled") "Device sync is switched off on the web player." else null,
                )
            }
            return null
        }
        val tls = socket["tls"]?.jsonPrimitive?.booleanOrNull ?: true
        val port = socket["port"]?.jsonPrimitive?.intOrNull ?: if (tls) 443 else 6001
        val url = "${if (tls) "wss" else "ws"}://${socket.string("host")}:$port/app/${socket.string("key")}?protocol=7&client=convx&version=1.0"
        return SocketConfig(channel, url)
    }

    /** Suspends until the socket closes; [onJoined] runs once the channel accepts this device. */
    private suspend fun runSocket(config: SocketConfig, onJoined: () -> Unit) {
        val closed = CompletableDeferred<Unit>()
        val listener = object : WebSocketListener() {
            override fun onMessage(webSocket: WebSocket, text: String) {
                try {
                    onFrame(webSocket, config.channel, text, onJoined)
                } catch (e: Exception) {
                    Timber.tag(TAG).w(e, "Unreadable frame")
                }
            }

            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
                webSocket.close(1000, null)
            }

            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
                closed.completeExceptionally(IOException("Socket closed ($code)"))
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                closed.completeExceptionally(t as? IOException ?: IOException(t))
            }
        }
        val socket = http.newWebSocket(Request.Builder().url(config.url).build(), listener)
        try {
            closed.await()
        } finally {
            socket.close(1000, null)
        }
    }

    private fun onFrame(socket: WebSocket, channel: String, text: String, onJoined: () -> Unit) {
        val frame = json.parseToJsonElement(text).jsonObject
        val data = frame["data"]?.let(::unwrap)
        when (frame.string("event")) {
            "pusher:connection_established" -> {
                val socketId = data?.jsonObject?.string("socket_id") ?: return
                scope.launch(Dispatchers.IO) { subscribe(socket, channel, socketId) }
            }

            "pusher:ping" -> socket.send("""{"event":"pusher:pong","data":{}}""")

            "pusher:error" -> Timber.tag(TAG).w("Socket error: %s", data)

            "pusher_internal:subscription_succeeded" -> {
                val hash = data?.jsonObject?.get("presence")?.jsonObject?.get("hash") as? JsonObject
                _state.update {
                    it.copy(
                        status = Status.Connected,
                        message = null,
                        devices = hash.orEmpty().map { (id, info) -> deviceOf(id, info as? JsonObject) },
                    )
                }
                onJoined()
                // Asks whichever device is the output to describe the whole session now, not at its next heartbeat.
                publish(socket, channel, "client-hello", buildJsonObject { put("device", deviceId) })
            }

            "pusher_internal:subscription_error", "pusher:subscription_error" ->
                socket.close(1000, "subscription refused")

            "pusher_internal:member_added" -> {
                val member = data?.jsonObject ?: return
                val id = member.string("user_id") ?: return
                val device = deviceOf(id, member["user_info"] as? JsonObject)
                _state.update { state -> state.copy(devices = state.devices.filterNot { it.id == id } + device) }
            }

            "pusher_internal:member_removed" -> {
                val id = data?.jsonObject?.string("user_id") ?: return
                _state.update { state -> state.copy(devices = state.devices.filterNot { it.id == id }) }
            }

            "client-state" -> {
                val message = data as? JsonObject ?: return
                if (message.string("device") == deviceId) return
                _state.update { it.copy(remotePlayback = message) }
            }
        }
    }

    private fun subscribe(socket: WebSocket, channel: String, socketId: String) {
        try {
            val url = "$BASE_URL/api/music/sync/auth".toHttpUrl().newBuilder()
                .addQueryParameter("device", deviceId)
                .addQueryParameter("name", "${Build.MODEL} · Convx")
                .addQueryParameter("kind", "mobile")
                .build()
            val form = FormBody.Builder().add("socket_id", socketId).add("channel_name", channel).build()
            val auth = execute(authorized(Request.Builder().url(url).post(form)).build()) { response ->
                when {
                    response.code == 401 -> throw SyncException("This device was signed out.", signedOut = true)
                    !response.isSuccessful -> throw SyncException(errorOf(response) ?: "The music server refused this device.")
                }
                json.parseToJsonElement(response.body?.string().orEmpty()).jsonObject
            }
            val subscribe = buildJsonObject {
                put("event", "pusher:subscribe")
                put("data", buildJsonObject {
                    put("channel", channel)
                    put("auth", auth.string("auth"))
                    put("channel_data", auth.string("channel_data"))
                })
            }
            socket.send(subscribe.toString())
        } catch (e: SyncException) {
            _state.update { it.copy(message = e.message) }
            if (e.signedOut) unpair() else socket.close(1000, "auth refused")
        } catch (e: IOException) {
            socket.close(1000, "auth failed")
        }
    }

    private fun publish(socket: WebSocket, channel: String, event: String, data: JsonObject) {
        socket.send(
            buildJsonObject {
                put("event", event)
                put("channel", channel)
                put("data", data)
            }.toString()
        )
    }

    private fun deviceOf(id: String, info: JsonObject?) = Device(
        id = id,
        name = info?.string("name") ?: "Device",
        isMobile = info?.string("kind") == "mobile",
        isSelf = id == deviceId,
    )

    /** The socket sends `data` as a JSON string for its own events and as an object for client ones. */
    private fun unwrap(data: JsonElement): JsonElement =
        if (data is JsonPrimitive && data.isString) json.parseToJsonElement(data.content) else data

    private fun authorized(builder: Request.Builder): Request.Builder =
        builder.header("Cookie", "$SESSION_COOKIE=${token ?: throw SyncException("Not paired.", signedOut = true)}")

    private fun <T> execute(request: Request, read: (Response) -> T): T =
        http.newCall(request).execute().use(read)

    private fun errorOf(response: Response): String? =
        runCatching { json.parseToJsonElement(response.body?.string().orEmpty()).jsonObject.string("error") }.getOrNull()

    private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

    private companion object {
        const val TAG = "DeviceSync"
        const val BASE_URL = "https://music.princeprajapati.dev"
        const val SESSION_COOKIE = "music_session"
        /** Excluded from backups: the session it holds signs in as the linked account. */
        const val PREFERENCES = "music_device_sync"
        const val KEY_TOKEN = "session"
        const val KEY_DEVICE = "device_id"
        const val MIN_BACKOFF_MS = 2_000L
        const val MAX_BACKOFF_MS = 60_000L
        const val DISABLED_RECHECK_MS = 5 * 60_000L
        val JSON_TYPE = "application/json".toMediaType()
    }
}
