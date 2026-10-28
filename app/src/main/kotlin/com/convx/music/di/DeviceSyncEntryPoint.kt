/**
 * Convx Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.convx.music.di

import com.convx.music.devicesync.DeviceSyncPlayback
import dagger.hilt.EntryPoint
import dagger.hilt.InstallIn
import dagger.hilt.components.SingletonComponent

@EntryPoint
@InstallIn(SingletonComponent::class)
interface DeviceSyncEntryPoint {
    fun deviceSyncPlayback(): DeviceSyncPlayback
}
