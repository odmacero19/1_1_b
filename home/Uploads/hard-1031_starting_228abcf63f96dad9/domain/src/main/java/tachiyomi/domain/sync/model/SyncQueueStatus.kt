package tachiyomi.domain.sync.model

/** Mirrors `sync_delta_queue.status`. */
enum class SyncQueueStatus {
    /** Waiting for upload. At most one entry per record. */
    PENDING,

    /** Claimed by a push run that has not finished yet. */
    IN_FLIGHT,

    /** Parked after a permanent failure. Put back in line by [tachiyomi.domain.sync.repository.SyncQueueRepository.requeueFailed]. */
    FAILED,
}
