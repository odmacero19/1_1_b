package eu.kanade.tachiyomi.data.sync

import android.content.Context
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkerParameters
import eu.kanade.tachiyomi.util.system.workManager
import logcat.LogPriority
import tachiyomi.core.common.util.system.logcat
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.util.concurrent.TimeUnit

/**
 * Uploads the local changes waiting in `sync_delta_queue`.
 *
 * The job is "continuous" in two ways: [DeltaSyncManager] enqueues [startNow] whenever something new lands in
 * the queue, and [setupTask] keeps a periodic run around as a safety net (process death, missed wake-ups).
 */
class DeltaSyncPushJob(context: Context, workerParams: WorkerParameters) :
    CoroutineWorker(context, workerParams) {

    override suspend fun doWork(): Result {
        val engine = Injekt.get<DeltaSyncPushEngine>()
        return try {
            when (val outcome = engine.run()) {
                is DeltaSyncPushEngine.Outcome.Disabled -> Result.success()
                is DeltaSyncPushEngine.Outcome.Completed -> {
                    logcat(LogPriority.DEBUG) { "Delta push done: $outcome" }
                    Result.success()
                }
                // WorkManager applies its exponential backoff; the queue is consistent in the meantime.
                is DeltaSyncPushEngine.Outcome.Retry -> Result.retry()
                is DeltaSyncPushEngine.Outcome.Failed -> Result.failure()
            }
        } catch (e: kotlin.coroutines.cancellation.CancellationException) {
            throw e
        } catch (e: Exception) {
            logcat(LogPriority.ERROR, e) { "Delta push crashed" }
            Result.retry()
        }
    }

    companion object {
        private const val TAG = "DeltaSyncPush"
        private const val TAG_PERIODIC = "$TAG:periodic"

        private val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        /** Periodic safety net. 15 minutes is the minimum WorkManager allows. */
        fun setupTask(context: Context) {
            val request = PeriodicWorkRequestBuilder<DeltaSyncPushJob>(15, TimeUnit.MINUTES)
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG_PERIODIC)
                .build()
            context.workManager.enqueueUniquePeriodicWork(TAG_PERIODIC, ExistingPeriodicWorkPolicy.UPDATE, request)
        }

        /**
         * Runs a push as soon as the network allows. Requests made while a run is active are appended and
         * executed after it, so changes queued during an upload are not left waiting for the periodic run.
         */
        fun startNow(context: Context) {
            val request = OneTimeWorkRequestBuilder<DeltaSyncPushJob>()
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG)
                .build()
            context.workManager.enqueueUniqueWork(TAG, ExistingWorkPolicy.APPEND_OR_REPLACE, request)
        }

        fun cancel(context: Context) {
            context.workManager.cancelUniqueWork(TAG)
            context.workManager.cancelUniqueWork(TAG_PERIODIC)
        }
    }
}
