package eu.kanade.tachiyomi.data.sync

import eu.kanade.tachiyomi.data.sync.model.DeltaPushRequest
import eu.kanade.tachiyomi.network.HttpException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.protobuf.ProtoBuf
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.model.ClaimedDeltas
import tachiyomi.domain.sync.repository.SyncQueueRepository
import tachiyomi.domain.sync.service.SyncPreferences
import java.io.IOException
import kotlin.coroutines.cancellation.CancellationException

/**
 * Drains `sync_delta_queue`: claim a bounded batch -> load just those rows -> serialize them as a delta ->
 * upload -> acknowledge. The queue is only ever changed by short database transactions; the network call
 * happens while no transaction is open, so a slow or failing server can neither block the UI nor corrupt the
 * queue. Whole tables are never read: only the rows the queue points at.
 */
class DeltaSyncPushEngine(
    private val queue: SyncQueueRepository,
    private val remote: DeltaSyncRemote,
    private val preferences: SyncPreferences,
    private val protoBuf: ProtoBuf,
) {

    sealed interface Outcome {
        data object Disabled : Outcome

        /** The queue is drained. [parked] entries were rejected by the server and will not be retried. */
        data class Completed(val uploaded: Int, val parked: Int) : Outcome

        /** Transient failure. The entries are pending again, try later. */
        data class Retry(val uploaded: Int, val reason: String?) : Outcome

        /** The endpoint refuses our requests (auth, URL, ...). The entries of the request are parked. */
        data class Failed(val uploaded: Int, val reason: String?) : Outcome
    }

    private class SerializationFailure(cause: Throwable) : Exception("Could not serialize delta", cause)

    suspend fun run(): Outcome {
        if (!preferences.deltaSyncEnabled.get()) return Outcome.Disabled

        // Only one push run exists at a time (unique work), so anything IN_FLIGHT is a leftover of a dead run.
        queue.recoverInterrupted()

        var uploaded = 0
        var parked = 0
        while (true) {
            currentCoroutineContext().ensureActive()

            val claimed = queue.claimBatch(BATCH_SIZE)
            if (claimed.isEmpty) return Outcome.Completed(uploaded, parked)

            // Claimed entries are IN_FLIGHT and must leave that state on every path, including cancellation.
            val work = ArrayDeque(listOf(claimed))
            while (work.isNotEmpty()) {
                val batch = work.removeFirst()
                try {
                    currentCoroutineContext().ensureActive()
                    remote.push(serialize(batch))
                } catch (e: Throwable) {
                    val entryIds = batch.entryIds
                    val remainingIds = work.flatMap { it.entryIds }
                    when {
                        e is CancellationException -> {
                            withContext(NonCancellable) { queue.release(entryIds + remainingIds, "cancelled") }
                            throw e
                        }
                        e.isPayloadRejection() && entryIds.size > 1 -> {
                            // Something in the batch is not acceptable. Bisect to isolate it so the rest of
                            // the batch is not held hostage.
                            val (first, second) = batch.split()
                            work.addFirst(second)
                            work.addFirst(first)
                        }
                        e.isPayloadRejection() -> {
                            logcat(LogPriority.ERROR, e) { "Delta rejected permanently, parking $entryIds" }
                            withContext(NonCancellable) { queue.park(entryIds, e.message) }
                            parked += entryIds.size
                        }
                        e.isTransientSyncFailure() -> {
                            logcat(LogPriority.WARN, e) { "Delta push failed, will retry" }
                            // A request that never reached the server does not count against the entries.
                            val countAttempt = e !is IOException
                            withContext(NonCancellable) {
                                queue.release(entryIds + remainingIds, e.message, countAttempt)
                            }
                            return Outcome.Retry(uploaded, e.message)
                        }
                        else -> {
                            logcat(LogPriority.ERROR, e) { "Delta endpoint refused the upload" }
                            withContext(NonCancellable) {
                                queue.park(entryIds, e.message)
                                queue.release(remainingIds, e.message)
                            }
                            return Outcome.Failed(uploaded, e.message)
                        }
                    }
                    continue
                }

                // The server has the data. Should this acknowledgement fail, the entries stay IN_FLIGHT, are
                // recovered by the next run and delivered a second time, which the receiver tolerates because
                // applying a delta is idempotent on (version, last_modified_at).
                val entryIds = batch.entryIds
                try {
                    withContext(NonCancellable) { queue.acknowledge(entryIds) }
                } catch (e: Exception) {
                    logcat(LogPriority.ERROR, e) { "Could not acknowledge uploaded deltas" }
                    withContext(NonCancellable) { queue.release(work.flatMap { it.entryIds }, e.message) }
                    return Outcome.Retry(uploaded, e.message)
                }
                uploaded += entryIds.size
            }
        }
    }

    private fun serialize(batch: ClaimedDeltas): ByteArray {
        return try {
            val request = DeltaSyncMapper.toRequest(
                deviceId = preferences.deviceId(),
                claimed = batch,
                now = System.currentTimeMillis(),
            )
            protoBuf.encodeToByteArray(DeltaPushRequest.serializer(), request)
        } catch (e: Exception) {
            throw SerializationFailure(e)
        }
    }

    private fun Throwable.isPayloadRejection(): Boolean {
        return this is SerializationFailure ||
            (this is HttpException && code in PAYLOAD_REJECTION_CODES)
    }

    private fun ClaimedDeltas.split(): Pair<ClaimedDeltas, ClaimedDeltas> {
        val firstHalf = entryIds.take(entryIds.size / 2).toSet()
        return ClaimedDeltas(
            mangas.filter { it.queueEntryId in firstHalf },
            chapters.filter { it.queueEntryId in firstHalf },
        ) to ClaimedDeltas(
            mangas.filterNot { it.queueEntryId in firstHalf },
            chapters.filterNot { it.queueEntryId in firstHalf },
        )
    }

    companion object {
        /** Upper bound of rows per request. */
        const val BATCH_SIZE = 100

        private val PAYLOAD_REJECTION_CODES = setOf(400, 413, 415, 422)
    }
}
