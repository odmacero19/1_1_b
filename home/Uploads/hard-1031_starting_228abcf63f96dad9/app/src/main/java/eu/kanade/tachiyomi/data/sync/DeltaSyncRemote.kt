package eu.kanade.tachiyomi.data.sync

import eu.kanade.tachiyomi.data.sync.model.DeltaPullResponse
import eu.kanade.tachiyomi.data.sync.model.DeltaPushRequest
import eu.kanade.tachiyomi.network.GET
import eu.kanade.tachiyomi.network.HttpException
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.network.POST
import eu.kanade.tachiyomi.network.awaitSuccess
import kotlinx.serialization.protobuf.ProtoBuf
import okhttp3.CacheControl
import okhttp3.Headers
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import tachiyomi.domain.sync.service.SyncPreferences
import java.io.IOException

/** Transport of the delta sync. */
interface DeltaSyncRemote {

    /** Uploads [payload]; returns normally only when the server acknowledged it. */
    suspend fun push(payload: ByteArray)

    suspend fun pull(cursor: String, limit: Int): DeltaPullResponse
}

/** The endpoint is not a usable URL. Not worth retrying until the preference changes. */
class DeltaSyncConfigurationException(message: String) : IllegalStateException(message)

/**
 * Talks to the (mock) sync endpoint over HTTP using the shared [NetworkHelper] client.
 *
 * `POST <endpoint>/push`  application/x-protobuf  [DeltaPushRequest]  -> any 2xx
 * `GET  <endpoint>/pull`  ?cursor=&limit=&deviceId=                    -> 2xx with a [DeltaPullResponse]
 *                                                                        (204 / empty body: nothing new)
 */
class HttpDeltaSyncRemote(
    private val networkHelper: NetworkHelper,
    private val preferences: SyncPreferences,
    private val protoBuf: ProtoBuf,
) : DeltaSyncRemote {

    override suspend fun push(payload: ByteArray) {
        val url = endpointUrl("push")
        val request = POST(
            url = url,
            headers = Headers.headersOf("X-Device-Id", preferences.deviceId()),
            body = payload.toRequestBody(PROTOBUF),
            cache = CacheControl.FORCE_NETWORK,
        )
        networkHelper.client.newCall(request).awaitSuccess().close()
    }

    override suspend fun pull(cursor: String, limit: Int): DeltaPullResponse {
        val url = endpointUrl("pull").toHttpUrlOrNull()!!
            .newBuilder()
            .addQueryParameter("cursor", cursor)
            .addQueryParameter("limit", limit.toString())
            .addQueryParameter("deviceId", preferences.deviceId())
            .build()
        val request = GET(
            url = url,
            headers = Headers.headersOf("Accept", PROTOBUF_TYPE),
            cache = CacheControl.FORCE_NETWORK,
        )
        networkHelper.client.newCall(request).awaitSuccess().use { response ->
            val bytes = if (response.code == 204) ByteArray(0) else response.body.bytes()
            if (bytes.isEmpty()) return DeltaPullResponse()
            return try {
                protoBuf.decodeFromByteArray(DeltaPullResponse.serializer(), bytes)
            } catch (e: Exception) {
                throw IOException("Malformed delta response", e)
            }
        }
    }

    private fun endpointUrl(path: String): String {
        val base = preferences.deltaSyncEndpoint.get().trim().trimEnd('/')
        val url = "$base/$path"
        if (url.toHttpUrlOrNull() == null) {
            throw DeltaSyncConfigurationException("Invalid delta sync endpoint: '$base'")
        }
        return url
    }

    private companion object {
        const val PROTOBUF_TYPE = "application/x-protobuf"
        val PROTOBUF = PROTOBUF_TYPE.toMediaType()
    }
}

/** True for failures where trying again later can succeed. */
internal fun Throwable.isTransientSyncFailure(): Boolean = when (this) {
    is DeltaSyncConfigurationException -> false
    is HttpException -> code == 408 || code == 425 || code == 429 || code >= 500
    else -> true
}
