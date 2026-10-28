/**
 * Convx Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.convx.music.ui.player

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.remember
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.convx.music.R
import com.convx.music.devicesync.DeviceSyncPlayback
import com.convx.music.di.DeviceSyncEntryPoint
import com.convx.music.ui.component.SymbolIcon
import com.convx.music.ui.component.shapes.ContinuousRoundedRectangle
import com.convx.music.ui.component.sf
import com.convx.music.ui.ios.compat.Text
import com.convx.music.ui.ios.component.IosCheckmark
import dagger.hilt.android.EntryPointAccessors

@Composable
fun rememberDeviceSyncPlayback(): DeviceSyncPlayback {
    val context = LocalContext.current.applicationContext
    return remember(context) {
        EntryPointAccessors.fromApplication(context, DeviceSyncEntryPoint::class.java).deviceSyncPlayback()
    }
}

/**
 * The account's other devices as outputs, below the local ones. Picking one moves the music
 * there; while another device plays, its transport controls sit here too.
 */
@Composable
fun DeviceSyncRoutes(
    sync: DeviceSyncPlayback,
    state: DeviceSyncPlayback.UiState,
    onDismiss: () -> Unit,
) {
    if (!state.live || state.devices.isEmpty()) return
    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            stringResource(R.string.device_sync_route_header),
            color = Color.White.copy(alpha = 0.6f),
            fontSize = 13.sp,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.padding(start = 8.dp, top = 4.dp),
        )
        state.devices.forEach { device ->
            val selected = state.outputId == device.id
            Box(
                contentAlignment = Alignment.CenterStart,
                modifier = Modifier
                    .fillMaxWidth()
                    .height(48.dp)
                    .clip(ContinuousRoundedRectangle(percent = 50))
                    .background(if (selected) Color.White else Color.White.copy(alpha = 0.12f))
                    .clickable {
                        if (!selected) sync.transferTo(device.id)
                        onDismiss()
                    },
            ) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    modifier = Modifier.fillMaxWidth().padding(horizontal = 16.dp),
                ) {
                    SymbolIcon(
                        symbol = sf(if (device.isMobile) "iphone" else "laptopcomputer"),
                        contentDescription = null,
                        size = 20.dp,
                        tint = if (selected) Color.Black else Color.White.copy(alpha = 0.82f),
                    )
                    Spacer(Modifier.width(10.dp))
                    Text(
                        device.name,
                        color = if (selected) Color.Black else Color.White,
                        fontSize = 17.sp,
                        fontWeight = FontWeight.SemiBold,
                        maxLines = 1,
                        modifier = Modifier.weight(1f),
                    )
                    if (selected) IosCheckmark(color = Color.Black)
                }
            }
        }
        state.remote?.let { remote -> RemoteTransport(sync, remote) }
    }
}

@Composable
private fun RemoteTransport(sync: DeviceSyncPlayback, remote: DeviceSyncPlayback.Remote) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.SpaceEvenly,
        modifier = Modifier.fillMaxWidth().padding(top = 4.dp),
    ) {
        TransportButton("backward.fill", stringResource(R.string.device_sync_previous), enabled = remote.index > 0) { sync.skip(-1) }
        TransportButton(
            if (remote.isPlaying) "pause.fill" else "play.fill",
            stringResource(if (remote.isPlaying) R.string.device_sync_pause else R.string.device_sync_play),
        ) { sync.togglePlayPause() }
        TransportButton("forward.fill", stringResource(R.string.device_sync_next), enabled = remote.index < remote.queue.lastIndex) { sync.skip(1) }
    }
}

@Composable
private fun TransportButton(symbol: String, label: String, enabled: Boolean = true, onClick: () -> Unit) {
    Box(
        contentAlignment = Alignment.Center,
        modifier = Modifier
            .height(48.dp)
            .width(64.dp)
            .clip(ContinuousRoundedRectangle(percent = 50))
            .clickable(enabled = enabled, onClick = onClick),
    ) {
        SymbolIcon(
            symbol = sf(symbol),
            contentDescription = label,
            size = 24.dp,
            tint = Color.White.copy(alpha = if (enabled) 1f else 0.35f),
            bounceOnTouch = true,
        )
    }
}

/** "Playing on …" for the popover header while another device is the output. */
@Composable
fun deviceSyncRemoteLabel(remote: DeviceSyncPlayback.Remote): String =
    stringResource(R.string.device_sync_playing_on, remote.deviceName)

@Composable
fun rememberDeviceSyncUi(sync: DeviceSyncPlayback): DeviceSyncPlayback.UiState {
    val state by sync.ui.collectAsStateWithLifecycle()
    return state
}
