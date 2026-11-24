package eu.kanade.tachiyomi.data.sync

import android.content.Context
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import tachiyomi.domain.sync.repository.SyncQueueRepository
import tachiyomi.domain.sync.service.SyncPreferences
import kotlin.time.Duration.Companion.seconds

/**
 * Wires the sync jobs to the app lifecycle:
 *  - schedules the periodic push / pull runs while the feature is enabled,
 *  - starts a push shortly after a *new* entry shows up in the queue (debounced so a burst of edits, e.g.
 *    "mark all as read", is uploaded as one batch),
 *  - puts parked entries back in line when the app starts or the endpoint changes.
 */
@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
class DeltaSyncManager(
    private val context: Context,
    private val queue: SyncQueueRepository,
    private val preferences: SyncPreferences,
) {

    fun init(scope: CoroutineScope) {
        preferences.deltaSyncEnabled.changes()
            .distinctUntilChanged()
            .onEach { enabled ->
                if (enabled) {
                    DeltaSyncPushJob.setupTask(context)
                    DeltaSyncPullJob.setupTask(context)
                    DeltaSyncPullJob.startNow(context)
                    DeltaSyncPushJob.startNow(context)
                } else {
                    DeltaSyncPushJob.cancel(context)
                    DeltaSyncPullJob.cancel(context)
                }
            }
            .launchIn(scope)

        // A new endpoint deserves a fresh try with everything that was parked for the old one.
        preferences.deltaSyncEndpoint.changes()
            .distinctUntilChanged()
            .drop(1)
            .onEach {
                try {
                    queue.requeueFailed()
                    if (preferences.deltaSyncEnabled.get()) DeltaSyncPushJob.startNow(context)
                } catch (e: Exception) {
                    logcat(LogPriority.ERROR, e) { "Could not requeue failed deltas" }
                }
            }
            .launchIn(scope)

        scope.launch {
            try {
                queue.requeueFailed()
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Could not requeue failed deltas" }
            }
        }

        preferences.deltaSyncEnabled.changes()
            .distinctUntilChanged()
            .flatMapLatest { enabled -> if (enabled) newEntryTicks() else emptyFlow() }
            .debounce(PUSH_DEBOUNCE)
            .onEach { DeltaSyncPushJob.startNow(context) }
            .launchIn(scope)
    }

    /**
     * Emits whenever an entry that was never seen before is enqueued. Retries (entries going back to PENDING)
     * keep their id and therefore do not wake the push job, this is what keeps WorkManager's backoff intact.
     */
    private fun newEntryTicks(): Flow<Long> {
        return flow {
            var highest = NOTHING_SEEN
            queue.latestEntryIdAsFlow()
                .distinctUntilChanged()
                .collect { id ->
                    // The first value is only the baseline: what is already queued is handled by the push that
                    // is started when the feature is enabled.
                    if (highest != NOTHING_SEEN && id > highest) emit(id)
                    highest = maxOf(highest, id)
                }
        }
            .catch { logcat(LogPriority.ERROR, it) { "Delta queue observer failed" } }
    }

    private companion object {
        val PUSH_DEBOUNCE = 2.seconds
        const val NOTHING_SEEN = -1L
    }
}
