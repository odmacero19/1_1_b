package eu.kanade.tachiyomi.data.sync

import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.model.RemoteApplyResult
import tachiyomi.domain.sync.repository.SyncRecordRepository
import tachiyomi.domain.sync.service.SyncPreferences
import kotlin.coroutines.cancellation.CancellationException

/**
 * Fetches remote deltas page by page and writes them via [SyncRecordRepository], which applies every record in
 * its own transaction with `is_syncing = 1` so the version triggers do not treat incoming changes as local edits
 * (which would queue them for upload again and ping-pong between devices forever).
 *
 * Applying a delta is idempotent, therefore the cursor is only persisted after a page was fully applied: a crash
 * in between just replays the page without effect.
 */
class DeltaSyncPullEngine(
    private val remote: DeltaSyncRemote,
    private val records: SyncRecordRepository,
    private val preferences: SyncPreferences,
) {

    sealed interface Outcome {
        data object Disabled : Outcome

        data class Completed(val applied: Int, val skipped: Int, val dropped: Int) : Outcome

        /** Transient failure, nothing lost: the cursor still points before the unapplied changes. */
        data class Retry(val applied: Int, val reason: String?) : Outcome

        data class Failed(val applied: Int, val reason: String?) : Outcome
    }

    /**
     * @param runAttempt how often this pull has been attempted in a row. Records that repeatedly cannot be
     * applied are skipped once it reaches [MAX_RECORD_ATTEMPTS] so one bad record cannot stall the feed.
     */
    suspend fun run(runAttempt: Int = 0): Outcome {
        if (!preferences.deltaSyncEnabled.get()) return Outcome.Disabled

        // Rows silenced forever by writers that never reset the flag would not be visible to local tracking.
        try {
            records.clearStaleSyncFlags()
        } catch (e: Exception) {
            logcat(LogPriority.WARN, e) { "Could not clear stale is_syncing flags" }
        }

        var applied = 0
        var skipped = 0
        var dropped = 0
        var cursor = preferences.deltaSyncPullCursor.get()

        // Guards against a server that keeps claiming hasMore without advancing.
        var pages = 0
        while (pages++ < MAX_PAGES_PER_RUN) {
            currentCoroutineContext().ensureActive()

            val response = try {
                remote.pull(cursor, PAGE_SIZE)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logcat(LogPriority.WARN, e) { "Delta pull failed" }
                return if (e.isTransientSyncFailure()) {
                    Outcome.Retry(applied, e.message)
                } else {
                    Outcome.Failed(applied, e.message)
                }
            }

            var failed = 0
            var lastError: Throwable? = null

            // Parents first, chapters may refer to a manga that is part of the same page.
            for (delta in response.mangas) {
                currentCoroutineContext().ensureActive()
                try {
                    when (records.applyRemoteManga(DeltaSyncMapper.fromWire(delta))) {
                        RemoteApplyResult.INSERTED, RemoteApplyResult.UPDATED -> applied++
                        RemoteApplyResult.STALE, RemoteApplyResult.MISSING_PARENT -> skipped++
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    failed++
                    lastError = e
                    logcat(LogPriority.ERROR, e) { "Could not apply remote manga delta" }
                }
            }
            for (delta in response.chapters) {
                currentCoroutineContext().ensureActive()
                try {
                    when (records.applyRemoteChapter(DeltaSyncMapper.fromWire(delta))) {
                        RemoteApplyResult.INSERTED, RemoteApplyResult.UPDATED -> applied++
                        RemoteApplyResult.STALE, RemoteApplyResult.MISSING_PARENT -> skipped++
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    failed++
                    lastError = e
                    logcat(LogPriority.ERROR, e) { "Could not apply remote chapter delta" }
                }
            }

            if (failed > 0) {
                if (runAttempt + 1 < MAX_RECORD_ATTEMPTS) {
                    // Do not move the cursor; the successfully applied part is replayed as no-ops.
                    return Outcome.Retry(applied, lastError?.message)
                }
                dropped += failed
                logcat(LogPriority.ERROR) { "Giving up on $failed remote delta(s) that cannot be applied" }
            }

            if (response.cursor.isNotEmpty()) {
                cursor = response.cursor
                preferences.deltaSyncPullCursor.set(cursor)
            }
            if (!response.hasMore) break
        }

        return Outcome.Completed(applied, skipped, dropped)
    }

    companion object {
        const val PAGE_SIZE = 200
        const val MAX_RECORD_ATTEMPTS = 5

        private const val MAX_PAGES_PER_RUN = 50
    }
}
