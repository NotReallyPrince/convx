package com.convx.music.devicesync

import org.junit.Assert.assertEquals
import org.junit.Test

class DeviceSyncPlaybackTest {
    // Expected values come from the web player's queueHash; a mismatch makes every device re-request the queue.
    @Test
    fun queueHashMatchesWebPlayer() {
        assertEquals("3:1iy37lj", DeviceSyncPlayback.queueHash(listOf("dQw4w9WgXcQ", "kJQP7kiw5Fk", "9bZkp7q19f0")))
        assertEquals("0:ztntfp", DeviceSyncPlayback.queueHash(emptyList()))
        assertEquals("1:1r9wi7g", DeviceSyncPlayback.queueHash(listOf("a")))
    }
}
