package tachiyomi.data.sync

import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOne
import app.cash.sqldelight.async.coroutines.awaitAsOneOrNull
import tachiyomi.data.Database
import tachiyomi.data.manga.MangaMapper
import tachiyomi.domain.manga.model.Manga
import tachiyomi.domain.sync.model.RemoteApplyResult
import tachiyomi.domain.sync.model.RemoteChapterDelta
import tachiyomi.domain.sync.model.RemoteMangaDelta
import tachiyomi.domain.sync.model.SyncTable
import tachiyomi.domain.sync.repository.SyncRecordRepository

class SyncRecordRepositoryImpl(
    private val database: Database,
) : SyncRecordRepository {

    private val queue
        get() = database.sync_delta_queueQueries

    override suspend fun applyRemoteManga(delta: RemoteMangaDelta): RemoteApplyResult {
        val remote = delta.manga
        return database.transactionWithResult {
            val local = database.mangasQueries
                .getMangaByUrlAndSource(remote.url, remote.source, MangaMapper::mapManga)
                .awaitAsOneOrNull()

            if (local != null && !remoteWins(remote.version, remote.lastModifiedAt, local.version, local.lastModifiedAt)) {
                return@transactionWithResult RemoteApplyResult.STALE
            }

            val mangaId = local?.id ?: insertManga(remote)

            // is_syncing = 1 is set by the same statement that writes the data, so the version triggers are
            // silent for everything below (including the mangas_categories triggers) until it is cleared.
            queue.applyRemoteManga(
                artist = remote.artist,
                author = remote.author,
                description = remote.description,
                genre = remote.genre,
                title = remote.title,
                status = remote.status,
                thumbnailUrl = remote.thumbnailUrl,
                favorite = remote.favorite,
                initialized = remote.initialized,
                viewerFlags = remote.viewerFlags,
                chapterFlags = remote.chapterFlags,
                dateAdded = remote.dateAdded,
                updateStrategy = remote.updateStrategy,
                notes = remote.notes,
                memo = remote.memo,
                version = remote.version,
                mangaId = mangaId,
            )

            delta.categoryNames?.let { replaceCategories(mangaId, it) }

            queue.clearMangaIsSyncing(mangaId)

            // Remote won: a local edit that was still waiting for upload is superseded.
            queue.deletePendingForRecord(SyncTable.MANGAS.tableName, mangaId)

            if (local == null) RemoteApplyResult.INSERTED else RemoteApplyResult.UPDATED
        }
    }

    override suspend fun applyRemoteChapter(delta: RemoteChapterDelta): RemoteApplyResult {
        val remote = delta.chapter
        return database.transactionWithResult {
            val manga = database.mangasQueries
                .getMangaByUrlAndSource(delta.mangaUrl, delta.mangaSource, MangaMapper::mapManga)
                .awaitAsOneOrNull()
                ?: return@transactionWithResult RemoteApplyResult.MISSING_PARENT

            val local = database.chaptersQueries
                .getChapterByUrlAndMangaId(remote.url, manga.id, ::mapChapter)
                .awaitAsOneOrNull()

            if (local == null) {
                // Inserts do not fire the version triggers, nothing has to be suppressed.
                database.chaptersQueries.insertReturningId(
                    manga.id,
                    remote.url,
                    remote.name,
                    remote.scanlator,
                    remote.read,
                    remote.bookmark,
                    remote.lastPageRead,
                    remote.chapterNumber,
                    remote.sourceOrder,
                    remote.dateFetch,
                    remote.dateUpload,
                    remote.version,
                    remote.memo,
                ).awaitAsOne()
                return@transactionWithResult RemoteApplyResult.INSERTED
            }

            if (!remoteWins(remote.version, remote.lastModifiedAt, local.version, local.lastModifiedAt)) {
                return@transactionWithResult RemoteApplyResult.STALE
            }

            queue.applyRemoteChapter(
                name = remote.name,
                scanlator = remote.scanlator,
                read = remote.read,
                bookmark = remote.bookmark,
                lastPageRead = remote.lastPageRead,
                chapterNumber = remote.chapterNumber,
                sourceOrder = remote.sourceOrder,
                dateUpload = remote.dateUpload,
                memo = remote.memo,
                version = remote.version,
                chapterId = local.id,
            )
            queue.clearChapterIsSyncing(local.id)
            queue.deletePendingForRecord(SyncTable.CHAPTERS.tableName, local.id)

            RemoteApplyResult.UPDATED
        }
    }

    override suspend fun clearStaleSyncFlags() {
        database.transaction {
            database.mangasQueries.resetIsSyncing()
            database.chaptersQueries.resetIsSyncing()
        }
    }

    private suspend fun insertManga(remote: Manga): Long {
        return database.mangasQueries.insertReturningId(
            source = remote.source,
            url = remote.url,
            artist = remote.artist,
            author = remote.author,
            description = remote.description,
            genre = remote.genre,
            title = remote.title,
            status = remote.status,
            thumbnailUrl = remote.thumbnailUrl,
            favorite = remote.favorite,
            lastUpdate = remote.lastUpdate,
            nextUpdate = 0L,
            calculateInterval = 0L,
            initialized = remote.initialized,
            viewerFlags = remote.viewerFlags,
            chapterFlags = remote.chapterFlags,
            coverLastModified = remote.coverLastModified,
            dateAdded = remote.dateAdded,
            updateStrategy = remote.updateStrategy,
            version = remote.version,
            notes = remote.notes,
            memo = remote.memo,
        ).awaitAsOne()
    }

    /** Must be called while the manga has is_syncing = 1. Categories this device does not have are skipped. */
    private suspend fun replaceCategories(mangaId: Long, names: List<String>) {
        val categoryIds = if (names.isEmpty()) {
            emptyList()
        } else {
            queue.getCategoryIdsByNames(names).awaitAsList()
        }
        database.mangas_categoriesQueries.deleteMangaCategoryByMangaId(mangaId)
        categoryIds.distinct().forEach { database.mangas_categoriesQueries.insert(mangaId, it) }
    }

    companion object {
        /**
         * Last-writer-wins. The version counts dirty transitions so the higher one carries more history;
         * equal versions fall back to the modification time. Equal on both means nothing to do, which also
         * makes re-delivery of a delta harmless.
         */
        internal fun remoteWins(
            remoteVersion: Long,
            remoteModifiedAt: Long,
            localVersion: Long,
            localModifiedAt: Long,
        ): Boolean {
            return remoteVersion > localVersion ||
                (remoteVersion == localVersion && remoteModifiedAt > localModifiedAt)
        }
    }
}
