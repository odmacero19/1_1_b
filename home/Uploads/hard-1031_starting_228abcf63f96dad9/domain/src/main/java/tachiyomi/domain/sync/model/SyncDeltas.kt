package tachiyomi.domain.sync.model

import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.manga.model.Manga

/** A dirty manga row that has been claimed for upload, together with the data that is not part of the row. */
data class LocalMangaDelta(
    val queueEntryId: Long,
    val manga: Manga,
    val categoryNames: List<String>,
)

/**
 * A dirty chapter row that has been claimed for upload. Row ids are device specific, so the owning manga is
 * identified by its (source, url) natural key.
 */
data class LocalChapterDelta(
    val queueEntryId: Long,
    val chapter: Chapter,
    val mangaSource: Long,
    val mangaUrl: String,
)

/**
 * Result of claiming queue entries. Every id in [entryIds] is IN_FLIGHT and has to be passed to exactly one of
 * acknowledge / release / park afterwards.
 */
data class ClaimedDeltas(
    val mangas: List<LocalMangaDelta>,
    val chapters: List<LocalChapterDelta>,
) {
    val entryIds: List<Long>
        get() = mangas.map { it.queueEntryId } + chapters.map { it.queueEntryId }

    val isEmpty: Boolean
        get() = mangas.isEmpty() && chapters.isEmpty()

    companion object {
        val EMPTY = ClaimedDeltas(emptyList(), emptyList())
    }
}

/** A manga change received from the remote. [categoryNames] is null when the remote does not describe categories. */
data class RemoteMangaDelta(
    val manga: Manga,
    val categoryNames: List<String>?,
)

/** A chapter change received from the remote. */
data class RemoteChapterDelta(
    val mangaSource: Long,
    val mangaUrl: String,
    val chapter: Chapter,
)

enum class RemoteApplyResult {
    /** A row that did not exist locally has been created. */
    INSERTED,

    /** The remote version won and has been written. */
    UPDATED,

    /** The local row is at least as new, nothing was written. */
    STALE,

    /** The chapter's manga does not exist locally (yet), nothing was written. */
    MISSING_PARENT,
}
