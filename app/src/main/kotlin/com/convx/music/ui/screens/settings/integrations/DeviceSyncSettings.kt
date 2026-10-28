/**
 * Convx Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.convx.music.ui.screens.settings.integrations

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.TopAppBarScrollBehavior
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.alpha
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.input.KeyboardCapitalization
import androidx.compose.ui.unit.dp
import androidx.hilt.lifecycle.viewmodel.compose.hiltViewModel
import androidx.lifecycle.ViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewModelScope
import androidx.navigation.NavController
import com.convx.music.R
import com.convx.music.devicesync.DeviceSyncClient
import com.convx.music.ui.component.IosSettingsGroup
import com.convx.music.ui.component.IosSettingsItem
import com.convx.music.ui.ios.compat.AlertDialog
import com.convx.music.ui.ios.compat.AppTheme
import com.convx.music.ui.ios.compat.CircularProgressIndicator
import com.convx.music.ui.ios.compat.OutlinedButton
import com.convx.music.ui.ios.compat.OutlinedTextField
import com.convx.music.ui.ios.compat.Text
import com.convx.music.ui.ios.compat.TextButton
import com.convx.music.ui.screens.settings.SettingsPage
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import java.io.IOException
import javax.inject.Inject

@HiltViewModel
class DeviceSyncSettingsViewModel @Inject constructor(
    private val client: DeviceSyncClient,
) : ViewModel() {
    val state: StateFlow<DeviceSyncClient.State> = client.state

    val isPairing = MutableStateFlow(false)
    val pairError = MutableStateFlow<String?>(null)

    fun pair(input: String, onPaired: () -> Unit) {
        if (input.isBlank() || isPairing.value) return
        isPairing.value = true
        pairError.value = null
        viewModelScope.launch {
            try {
                client.pair(input)
                onPaired()
            } catch (e: CancellationException) {
                throw e
            } catch (e: DeviceSyncClient.SyncException) {
                pairError.value = e.message
            } catch (e: IOException) {
                pairError.value = "Couldn't reach the music server."
            } finally {
                isPairing.value = false
            }
        }
    }

    fun clearError() {
        pairError.value = null
    }

    fun unpair() = client.unpair()
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun DeviceSyncSettings(
    navController: NavController,
    @Suppress("UNUSED_PARAMETER") scrollBehavior: TopAppBarScrollBehavior,
    viewModel: DeviceSyncSettingsViewModel = hiltViewModel(),
) {
    val state by viewModel.state.collectAsStateWithLifecycle()
    val isPairing by viewModel.isPairing.collectAsStateWithLifecycle()
    val pairError by viewModel.pairError.collectAsStateWithLifecycle()
    var showPairDialog by rememberSaveable { mutableStateOf(false) }
    val isPaired = state.status != DeviceSyncClient.Status.SignedOut

    if (showPairDialog) {
        var input by rememberSaveable { mutableStateOf("") }
        val dismiss = {
            if (!isPairing) {
                showPairDialog = false
                viewModel.clearError()
            }
        }

        AlertDialog(
            onDismissRequest = dismiss,
            title = { Text(stringResource(R.string.device_sync_pair_title)) },
            text = {
                Column(verticalArrangement = Arrangement.spacedBy(12.dp)) {
                    Text(
                        text = stringResource(R.string.device_sync_pair_hint),
                        style = AppTheme.typography.bodySmall,
                    )
                    OutlinedTextField(
                        value = input,
                        onValueChange = {
                            input = it
                            viewModel.clearError()
                        },
                        label = { Text(stringResource(R.string.device_sync_code_label)) },
                        singleLine = true,
                        enabled = !isPairing,
                        keyboardOptions = KeyboardOptions(capitalization = KeyboardCapitalization.Characters),
                        modifier = Modifier.fillMaxWidth(),
                    )
                    pairError?.let {
                        Text(text = it, color = AppTheme.colorScheme.error, style = AppTheme.typography.bodySmall)
                    }
                    if (isPairing) CircularProgressIndicator(modifier = Modifier.size(16.dp))
                }
            },
            confirmButton = {
                TextButton(
                    onClick = { viewModel.pair(input) { showPairDialog = false } },
                    enabled = !isPairing && input.isNotBlank(),
                ) {
                    Text(stringResource(R.string.device_sync_pair_action))
                }
            },
            dismissButton = {
                TextButton(onClick = dismiss, enabled = !isPairing) {
                    Text(stringResource(R.string.cancel))
                }
            },
        )
    }

    SettingsPage(
        navController = navController,
        title = stringResource(R.string.device_sync_integration),
        pageColor = AppTheme.colorScheme.background,
        horizontalPadding = 16.dp,
    ) {
        IosSettingsGroup(
            title = stringResource(R.string.account),
            items = listOf(
                IosSettingsItem(
                    icon = painterResource(R.drawable.sync),
                    title = {
                        Text(
                            text = stringResource(statusLabel(state.status)),
                            modifier = Modifier.alpha(if (isPaired) 1f else 0.5f),
                        )
                    },
                    description = state.message?.let { message -> @Composable { Text(message) } },
                    trailingContent = {
                        if (isPaired) {
                            OutlinedButton(onClick = viewModel::unpair) {
                                Text(stringResource(R.string.device_sync_unpair_action))
                            }
                        } else {
                            OutlinedButton(onClick = { showPairDialog = true }) {
                                Text(stringResource(R.string.device_sync_pair_action))
                            }
                        }
                    },
                ),
            ),
        )

        if (state.devices.isNotEmpty()) {
            Spacer(modifier = Modifier.height(16.dp))
            IosSettingsGroup(
                title = stringResource(R.string.device_sync_devices),
                items = state.devices.map { device ->
                    IosSettingsItem(
                        icon = painterResource(if (device.isMobile) R.drawable.cast else R.drawable.web_link),
                        title = { Text(device.name) },
                        description = if (device.isSelf) {
                            @Composable { Text(stringResource(R.string.device_sync_this_device)) }
                        } else {
                            null
                        },
                    )
                },
            )
        }
    }
}

private fun statusLabel(status: DeviceSyncClient.Status): Int = when (status) {
    DeviceSyncClient.Status.SignedOut -> R.string.device_sync_status_signed_out
    DeviceSyncClient.Status.Connecting -> R.string.device_sync_status_connecting
    DeviceSyncClient.Status.Connected -> R.string.device_sync_status_connected
    DeviceSyncClient.Status.Disabled -> R.string.device_sync_status_disabled
    DeviceSyncClient.Status.Offline -> R.string.device_sync_status_offline
}
