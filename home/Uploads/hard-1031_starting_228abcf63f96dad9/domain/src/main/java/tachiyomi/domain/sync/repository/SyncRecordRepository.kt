package tachiyomi.domain.sync.repository

import tachiyomi.domain.sync.model.RemoteApplyResult
import tachiyomi.domain.sync.model.RemoteChapterDelta
import tachiyomi.domain.sync.model.RemoteMangaDelta

/**
 * Writes changes received from the remote into the local database.
 *
 * Each call is one transaction during which the affected row has `is_syncing = 1`, so the version triggers
 * neither bump the row nor queue it for upload again. Conflicts are resolved last-writer-wins on
 * (version, last_modified_at); when the remote wins, a not yet uploaded local change to the same row is dropped.
 */
interface SyncRecordRepository {

    suspend fun applyRemoteManga(delta: RemoteMangaDelta): RemoteApplyResult

    suspend fun applyRemoteChapter(delta: RemoteChapterDelta): RemoteApplyResult

    /**
     * Clears `is_syncing` flags that were left set by writers that predate the sync engine (which kept the
     * flag on forever and thereby silenced change tracking for those rows).
     */
    suspend fun clearStaleSyncFlags()
}
