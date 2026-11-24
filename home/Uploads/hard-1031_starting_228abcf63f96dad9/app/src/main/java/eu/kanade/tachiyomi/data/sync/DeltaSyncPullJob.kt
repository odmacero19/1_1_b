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
import kotlin.coroutines.cancellation.CancellationException

/** Downloads remote deltas and applies them to the local database with change tracking suppressed. */
class DeltaSyncPullJob(context: Context, workerParams: WorkerParameters) :
    CoroutineWorker(context, workerParams) {

    override suspend fun doWork(): Result {
        val engine = Injekt.get<DeltaSyncPullEngine>()
        return try {
            when (val outcome = engine.run(runAttempt)) {
                is DeltaSyncPullEngine.Outcome.Disabled -> Result.success()
                is DeltaSyncPullEngine.Outcome.Completed -> {
                    logcat(LogPriority.DEBUG) { "Delta pull done: $outcome" }
                    Result.success()
                }
                is DeltaSyncPullEngine.Outcome.Retry -> Result.retry()
                is DeltaSyncPullEngine.Outcome.Failed -> Result.failure()
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logcat(LogPriority.ERROR, e) { "Delta pull crashed" }
            Result.retry()
        }
    }

    companion object {
        private const val TAG = "DeltaSyncPull"
        private const val TAG_PERIODIC = "$TAG:periodic"

        private val constraints = Constraints.Builder()
            .setRequiredNetworkType(NetworkType.CONNECTED)
            .build()

        fun setupTask(context: Context) {
            val request = PeriodicWorkRequestBuilder<DeltaSyncPullJob>(15, TimeUnit.MINUTES)
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG_PERIODIC)
                .build()
            context.workManager.enqueueUniquePeriodicWork(TAG_PERIODIC, ExistingPeriodicWorkPolicy.UPDATE, request)
        }

        fun startNow(context: Context) {
            val request = OneTimeWorkRequestBuilder<DeltaSyncPullJob>()
                .setConstraints(constraints)
                .setBackoffCriteria(BackoffPolicy.EXPONENTIAL, 30, TimeUnit.SECONDS)
                .addTag(TAG)
                .build()
            context.workManager.enqueueUniqueWork(TAG, ExistingWorkPolicy.KEEP, request)
        }

        fun cancel(context: Context) {
            context.workManager.cancelUniqueWork(TAG)
            context.workManager.cancelUniqueWork(TAG_PERIODIC)
        }
    }
}
