package tachiyomi.domain.sync.repository

import kotlinx.coroutines.flow.Flow
import tachiyomi.domain.sync.model.ClaimedDeltas
import tachiyomi.domain.sync.model.SyncTable

/**
 * Access to the local outbox (`sync_delta_queue`).
 *
 * Every function is atomic. The intended protocol of a push run is:
 *
 * 1. [recoverInterrupted]
 * 2. [claimBatch]              (entries PENDING -> IN_FLIGHT, rows are read in the same transaction)
 * 3. upload
 * 4. [acknowledge] on success, [release] on a transient failure or [park] on a permanent one
 *
 * Nothing is ever deleted from the queue before the server acknowledged the upload.
 */
interface SyncQueueRepository {

    /**
     * Atomically claims up to [limit] of the oldest pending entries and loads the rows they point to. Entries
     * whose row no longer exists are removed, they have nothing left to upload.
     */
    suspend fun claimBatch(limit: Int): ClaimedDeltas

    /** The server accepted these entries; remove them. */
    suspend fun acknowledge(entryIds: List<Long>)

    /**
     * Transient failure: make the entries pending again. With [countAttempt] the failure is held against the
     * entries, after too many of those they are parked; offline failures should not be counted.
     */
    suspend fun release(entryIds: List<Long>, error: String?, countAttempt: Boolean = true)

    /** Permanent failure: stop retrying these entries until [requeueFailed]. */
    suspend fun park(entryIds: List<Long>, error: String?)

    /** Turns entries left IN_FLIGHT by a run that died back into PENDING entries. */
    suspend fun recoverInterrupted()

    /** Puts parked entries back in line. */
    suspend fun requeueFailed()

    suspend fun enqueue(table: SyncTable, recordId: Long)

    suspend fun countPending(): Long

    suspend fun countFailed(): Long

    /** Emits the highest queue id ever used, which only increases when something new is enqueued. */
    fun latestEntryIdAsFlow(): Flow<Long>
}
