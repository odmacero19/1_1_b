package eu.kanade.tachiyomi.data.sync.model

import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import eu.kanade.tachiyomi.data.backup.models.BackupManga
import kotlinx.serialization.Serializable
import kotlinx.serialization.protobuf.ProtoNumber

/*
 * Wire format of the delta sync. It re-uses the kotlinx-protobuf models of the backup format for the actual
 * row content (BackupManga / BackupChapter, so field numbers and defaults stay compatible), but only ever
 * carries rows that were queued in sync_delta_queue, never a whole library.
 */

/** Body of `POST <endpoint>/push`. */
@Serializable
class DeltaPushRequest(
    @ProtoNumber(1) val deviceId: String,
    @ProtoNumber(2) val sentAt: Long,
    @ProtoNumber(3) val mangas: List<MangaDelta> = emptyList(),
    @ProtoNumber(4) val chapters: List<ChapterDelta> = emptyList(),
)

/** Body of the response of `GET <endpoint>/pull?cursor=<cursor>&limit=<n>&deviceId=<id>`. */
@Serializable
class DeltaPullResponse(
    @ProtoNumber(1) val mangas: List<MangaDelta> = emptyList(),
    @ProtoNumber(2) val chapters: List<ChapterDelta> = emptyList(),
    /** Position to continue from. Empty: keep the previous one. */
    @ProtoNumber(3) val cursor: String = "",
    /** More changes are available right away. */
    @ProtoNumber(4) val hasMore: Boolean = false,
)

@Serializable
class MangaDelta(
    /** Only the manga columns; chapters, history and tracking are always empty. */
    @ProtoNumber(1) val manga: BackupManga,
    @ProtoNumber(2) val categories: List<String> = emptyList(),
    /** False when [categories] must not be interpreted (an empty list would mean "no categories"). */
    @ProtoNumber(3) val hasCategories: Boolean = true,
)

@Serializable
class ChapterDelta(
    /** Natural key of the owning manga. Local row ids are meaningless on another device. */
    @ProtoNumber(1) val mangaSource: Long,
    @ProtoNumber(2) val mangaUrl: String,
    @ProtoNumber(3) val chapter: BackupChapter,
)
