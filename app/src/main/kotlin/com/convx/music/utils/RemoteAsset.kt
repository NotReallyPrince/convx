package com.convx.music.utils

import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.security.DigestOutputStream
import java.security.MessageDigest

/**
 * Large files the APK no longer bundles, fetched from the `models` GitHub release on first use.
 * The hash pins each file, so a tampered or truncated download is never handed to a parser.
 */
object RemoteAsset {
    private const val BASE_URL = "https://github.com/NotReallyPrince/convx/releases/download/models/"
    private val client by lazy { OkHttpClient() }

    /** Blocking. Returns the cached file, downloading it first if needed; throws on any failure. */
    // ponytail: one lock for all assets, so a second asset waits behind the first download.
    @Synchronized
    fun get(dir: File, name: String, sha256: String): File {
        val file = File(dir, name)
        if (file.length() > 0L) return file
        val part = File(dir, "$name.part")
        val digest = MessageDigest.getInstance("SHA-256")
        try {
            client.newCall(Request.Builder().url(BASE_URL + name).build()).execute().use { response ->
                check(response.isSuccessful) { "HTTP ${response.code} fetching $name" }
                DigestOutputStream(part.outputStream(), digest).use { response.body.byteStream().copyTo(it) }
            }
            val actual = digest.digest().joinToString("") { "%02x".format(it) }
            check(actual == sha256) { "$name checksum mismatch" }
            check(part.renameTo(file)) { "could not store $name" }
        } finally {
            part.delete()
        }
        return file
    }
}
