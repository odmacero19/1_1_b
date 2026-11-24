package eu.kanade.tachiyomi.data.sync

import eu.kanade.tachiyomi.data.backup.create.creators.toBackupManga
import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import eu.kanade.tachiyomi.data.sync.model.ChapterDelta
import eu.kanade.tachiyomi.data.sync.model.DeltaPushRequest
import eu.kanade.tachiyomi.data.sync.model.MangaDelta
import tachiyomi.data.MemoColumnAdapter
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.sync.model.ClaimedDeltas
import tachiyomi.domain.sync.model.LocalChapterDelta
import tachiyomi.domain.sync.model.LocalMangaDelta
import tachiyomi.domain.sync.model.RemoteChapterDelta
import tachiyomi.domain.sync.model.RemoteMangaDelta

/** Conversions between queue rows / domain models and the delta wire models. */
internal object DeltaSyncMapper {

    fun toRequest(deviceId: String, claimed: ClaimedDeltas, now: Long): DeltaPushRequest {
        return DeltaPushRequest(
            deviceId = deviceId,
            sentAt = now,
            mangas = claimed.mangas.map(::toWire),
            chapters = claimed.chapters.map(::toWire),
        )
    }

    fun toWire(local: LocalMangaDelta): MangaDelta {
        return MangaDelta(
            manga = local.manga.toBackupManga(),
            categories = local.categoryNames,
            hasCategories = true,
        )
    }

    fun toWire(local: LocalChapterDelta): ChapterDelta {
        return ChapterDelta(
            mangaSource = local.mangaSource,
            mangaUrl = local.mangaUrl,
            chapter = local.chapter.toBackupChapter(),
        )
    }

    fun fromWire(wire: MangaDelta): RemoteMangaDelta {
        return RemoteMangaDelta(
            manga = wire.manga.getMangaImpl(),
            categoryNames = wire.categories.takeIf { wire.hasCategories },
        )
    }

    fun fromWire(wire: ChapterDelta): RemoteChapterDelta {
        return RemoteChapterDelta(
            mangaSource = wire.mangaSource,
            mangaUrl = wire.mangaUrl,
            chapter = wire.chapter.toChapterImpl(),
        )
    }

    private fun Chapter.toBackupChapter(): BackupChapter {
        return BackupChapter(
            url = url,
            name = name,
            scanlator = scanlator,
            read = read,
            bookmark = bookmark,
            lastPageRead = lastPageRead,
            dateFetch = dateFetch,
            dateUpload = dateUpload,
            chapterNumber = chapterNumber.toFloat(),
            sourceOrder = sourceOrder,
            lastModifiedAt = lastModifiedAt,
            version = version,
            memo = MemoColumnAdapter.encode(memo),
        )
    }
}
