package tachiyomi.data.sync

import app.cash.sqldelight.async.coroutines.awaitAsList
import app.cash.sqldelight.async.coroutines.awaitAsOne
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.json.JsonObject
import tachiyomi.data.Database
import tachiyomi.data.manga.MangaMapper
import tachiyomi.data.subscribeToOne
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga
import tachiyomi.domain.sync.model.ClaimedDeltas
import tachiyomi.domain.sync.model.LocalChapterDelta
import tachiyomi.domain.sync.model.LocalMangaDelta
import tachiyomi.domain.sync.model.SyncQueueStatus
import tachiyomi.domain.sync.model.SyncTable
import tachiyomi.domain.sync.repository.SyncQueueRepository

class SyncQueueRepositoryImpl(
    private val database: Database,
) : SyncQueueRepository {

    private val queue
        get() = database.sync_delta_queueQueries

    private data class ClaimedEntry(val id: Long, val table: SyncTable?, val recordId: Long)

    override suspend fun claimBatch(limit: Int): ClaimedDeltas {
        // An entire batch can consist of entries whose rows were deleted in the meantime. Those are dropped by
        // claimOnce, so keep going until something uploadable turns up or the queue is empty.
        while (true) {
            val (claimedAny, deltas) = claimOnce(limit)
            if (!claimedAny) return ClaimedDeltas.EMPTY
            if (!deltas.isEmpty) return deltas
        }
    }

    private suspend fun claimOnce(limit: Int): Pair<Boolean, ClaimedDeltas> {
        // Claiming and reading the rows share one transaction: the snapshot that is uploaded is exactly
        // consistent with the entries that are going to be acknowledged.
        return database.transactionWithResult {
            queue.claimPending(limit.toLong())

            val entries = queue
                .getInFlight { id, tableName, recordId, _ ->
                    ClaimedEntry(id, SyncTable.fromTableName(tableName), recordId)
                }
                .awaitAsList()
            if (entries.isEmpty()) return@transactionWithResult false to ClaimedDeltas.EMPTY

            val mangaEntries = entries.filter { it.table == SyncTable.MANGAS }
            val chapterEntries = entries.filter { it.table == SyncTable.CHAPTERS }

            val mangasById = loadMangas(mangaEntries.map { it.recordId })
            val chaptersById = loadChapters(chapterEntries.map { it.recordId })
            val chapterParents = loadMangas(chaptersById.values.map { it.mangaId }.distinct())

            val mangaDeltas = mangaEntries.mapNotNull { entry ->
                val manga = mangasById[entry.recordId] ?: return@mapNotNull null
                LocalMangaDelta(
                    queueEntryId = entry.id,
                    manga = manga,
                    categoryNames = queue.getCategoryNamesByMangaId(manga.id).awaitAsList(),
                )
            }
            val chapterDeltas = chapterEntries.mapNotNull { entry ->
                val chapter = chaptersById[entry.recordId] ?: return@mapNotNull null
                val parent = chapterParents[chapter.mangaId] ?: return@mapNotNull null
                LocalChapterDelta(
                    queueEntryId = entry.id,
                    chapter = chapter,
                    mangaSource = parent.source,
                    mangaUrl = parent.url,
                )
            }

            // Whatever could not be resolved (deleted rows, unknown table) has nothing to upload.
            val resolved = (mangaDeltas.map { it.queueEntryId } + chapterDeltas.map { it.queueEntryId }).toSet()
            val unresolved = entries.map { it.id }.filterNot { it in resolved }
            unresolved.chunked(CHUNK).forEach { queue.deleteByIds(it) }

            true to ClaimedDeltas(mangaDeltas, chapterDeltas)
        }
    }

    override suspend fun acknowledge(entryIds: List<Long>) {
        if (entryIds.isEmpty()) return
        database.transaction {
            entryIds.chunked(CHUNK).forEach { queue.deleteByIds(it) }
        }
    }

    override suspend fun release(entryIds: List<Long>, error: String?, countAttempt: Boolean) {
        if (entryIds.isEmpty()) return
        database.transaction {
            entryIds.chunked(CHUNK).forEach { chunk ->
                queue.dropSupersededInFlight(chunk)
                if (countAttempt) {
                    queue.releaseInFlight(error?.take(MAX_ERROR_LENGTH), chunk)
                } else {
                    queue.releaseInFlightWithoutPenalty(error?.take(MAX_ERROR_LENGTH), chunk)
                }
            }
            queue.parkExhausted(MAX_ATTEMPTS)
        }
    }

    override suspend fun park(entryIds: List<Long>, error: String?) {
        if (entryIds.isEmpty()) return
        database.transaction {
            entryIds.chunked(CHUNK).forEach { chunk ->
                queue.dropSupersededInFlight(chunk)
                queue.parkInFlight(error?.take(MAX_ERROR_LENGTH), chunk)
            }
        }
    }

    override suspend fun recoverInterrupted() {
        database.transaction {
            queue.dropSupersededStaleInFlight()
            queue.recoverStaleInFlight()
        }
    }

    override suspend fun requeueFailed() {
        database.transaction {
            queue.dropFailedCoveredByPending()
            queue.dropFailedDuplicates()
            queue.requeueFailed()
        }
    }

    override suspend fun enqueue(table: SyncTable, recordId: Long) {
        queue.enqueue(table.tableName, recordId)
    }

    override suspend fun countPending(): Long {
        return queue.countByStatus(SyncQueueStatus.PENDING.name).awaitAsOne()
    }

    override suspend fun countFailed(): Long {
        return queue.countByStatus(SyncQueueStatus.FAILED.name).awaitAsOne()
    }

    override fun latestEntryIdAsFlow(): Flow<Long> {
        return queue.latestEntryId().subscribeToOne()
    }

    private suspend fun loadMangas(ids: List<Long>): Map<Long, Manga> {
        if (ids.isEmpty()) return emptyMap()
        return ids.chunked(CHUNK)
            .flatMap { queue.getMangasByIds(it, MangaMapper::mapManga).awaitAsList() }
            .associateBy { it.id }
    }

    private suspend fun loadChapters(ids: List<Long>): Map<Long, Chapter> {
        if (ids.isEmpty()) return emptyMap()
        return ids.chunked(CHUNK)
            .flatMap { queue.getChaptersByIds(it, ::mapChapter).awaitAsList() }
            .associateBy { it.id }
    }

    companion object {
        /** Entries that failed this many times are parked instead of being retried forever. */
        const val MAX_ATTEMPTS = 8L

        private const val CHUNK = 500
        private const val MAX_ERROR_LENGTH = 500
    }
}

@Suppress("UNUSED_PARAMETER", "LongParameterList")
internal fun mapChapter(
    id: Long,
    mangaId: Long,
    url: String,
    name: String,
    scanlator: String?,
    read: Boolean,
    bookmark: Boolean,
    lastPageRead: Long,
    chapterNumber: Double,
    sourceOrder: Long,
    dateFetch: Long,
    dateUpload: Long,
    lastModifiedAt: Long,
    version: Long,
    isSyncing: Long,
    memo: JsonObject,
): Chapter = Chapter(
    id = id,
    mangaId = mangaId,
    read = read,
    bookmark = bookmark,
    lastPageRead = lastPageRead,
    dateFetch = dateFetch,
    sourceOrder = sourceOrder,
    url = url,
    name = name,
    dateUpload = dateUpload,
    chapterNumber = chapterNumber,
    scanlator = scanlator,
    lastModifiedAt = lastModifiedAt,
    version = version,
    memo = memo,
)
