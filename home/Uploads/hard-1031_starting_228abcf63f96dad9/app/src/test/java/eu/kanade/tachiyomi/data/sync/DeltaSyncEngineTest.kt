package eu.kanade.tachiyomi.data.sync

import eu.kanade.tachiyomi.data.backup.models.BackupChapter
import eu.kanade.tachiyomi.data.sync.model.ChapterDelta
import eu.kanade.tachiyomi.data.sync.model.DeltaPullResponse
import eu.kanade.tachiyomi.data.sync.model.DeltaPushRequest
import eu.kanade.tachiyomi.network.HttpException
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.protobuf.ProtoBuf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tachiyomi.core.common.preference.Preference
import tachiyomi.domain.chapter.model.Chapter
import tachiyomi.domain.sync.model.ClaimedDeltas
import tachiyomi.domain.sync.model.LocalChapterDelta
import tachiyomi.domain.sync.model.RemoteApplyResult
import tachiyomi.domain.sync.model.RemoteChapterDelta
import tachiyomi.domain.sync.model.RemoteMangaDelta
import tachiyomi.domain.sync.model.SyncTable
import tachiyomi.domain.sync.repository.SyncQueueRepository
import tachiyomi.domain.sync.repository.SyncRecordRepository
import tachiyomi.domain.sync.service.SyncPreferences
import java.io.IOException

class DeltaSyncEngineTest {

    private val protoBuf = ProtoBuf

    private class MemoryPreference<T>(initial: T) {
        var value: T = initial
        val preference: Preference<T> = mockk {
            every { get() } answers { value }
            every { set(any()) } answers { value = firstArg() }
        }
    }

    private fun preferences(
        enabled: Boolean = true,
        cursor: MemoryPreference<String> = MemoryPreference(""),
    ): SyncPreferences = mockk {
        every { deltaSyncEnabled } returns MemoryPreference(enabled).preference
        every { deltaSyncPullCursor } returns cursor.preference
        every { deviceId() } returns "device"
    }

    private fun chapterDelta(entryId: Long, url: String = "/c/$entryId") = LocalChapterDelta(
        queueEntryId = entryId,
        chapter = Chapter.create().copy(id = entryId, url = url, name = "Chapter $entryId", read = true),
        mangaSource = 7,
        mangaUrl = "/manga",
    )

    private class FakeQueue(pending: List<LocalChapterDelta>) : SyncQueueRepository {
        private val waiting = pending.toMutableList()
        val acked = mutableListOf<Long>()
        val released = mutableListOf<Long>()
        val releaseCounted = mutableListOf<Boolean>()
        val parked = mutableListOf<Long>()

        override suspend fun claimBatch(limit: Int): ClaimedDeltas {
            val batch = waiting.take(limit)
            repeat(batch.size) { waiting.removeAt(0) }
            return ClaimedDeltas(emptyList(), batch)
        }

        override suspend fun acknowledge(entryIds: List<Long>) {
            acked += entryIds
        }

        override suspend fun release(entryIds: List<Long>, error: String?, countAttempt: Boolean) {
            released += entryIds
            releaseCounted += countAttempt
        }

        override suspend fun park(entryIds: List<Long>, error: String?) {
            parked += entryIds
        }

        override suspend fun recoverInterrupted() = Unit
        override suspend fun requeueFailed() = Unit
        override suspend fun enqueue(table: SyncTable, recordId: Long) = Unit
        override suspend fun countPending(): Long = waiting.size.toLong()
        override suspend fun countFailed(): Long = 0
        override fun latestEntryIdAsFlow(): Flow<Long> = flowOf(0)
    }

    private class FakeRemote(
        private val onPush: (ByteArray) -> Unit = {},
        private val onPull: (String) -> DeltaPullResponse = { DeltaPullResponse() },
    ) : DeltaSyncRemote {
        val pushes = mutableListOf<ByteArray>()
        override suspend fun push(payload: ByteArray) {
            pushes += payload
            onPush(payload)
        }

        override suspend fun pull(cursor: String, limit: Int): DeltaPullResponse = onPull(cursor)
    }

    @Test
    fun `push uploads only the queued rows and acknowledges them`() = runBlocking {
        val queue = FakeQueue((1L..3L).map { chapterDelta(it) })
        val remote = FakeRemote()
        val outcome = DeltaSyncPushEngine(queue, remote, preferences(), protoBuf).run()

        assertEquals(DeltaSyncPushEngine.Outcome.Completed(uploaded = 3, parked = 0), outcome)
        assertEquals(listOf(1L, 2L, 3L), queue.acked)
        assertEquals(1, remote.pushes.size)

        val request = protoBuf.decodeFromByteArray(DeltaPushRequest.serializer(), remote.pushes.single())
        assertEquals("device", request.deviceId)
        assertTrue(request.mangas.isEmpty())
        assertEquals(listOf("/c/1", "/c/2", "/c/3"), request.chapters.map { it.chapter.url })
        assertEquals(setOf("/manga"), request.chapters.map { it.mangaUrl }.toSet())
    }

    @Test
    fun `offline failure keeps entries and does not count against them`() = runBlocking {
        val queue = FakeQueue((1L..2L).map { chapterDelta(it) })
        val remote = FakeRemote(onPush = { throw IOException("offline") })
        val outcome = DeltaSyncPushEngine(queue, remote, preferences(), protoBuf).run()

        assertTrue(outcome is DeltaSyncPushEngine.Outcome.Retry)
        assertTrue(queue.acked.isEmpty())
        assertEquals(listOf(1L, 2L), queue.released)
        assertEquals(listOf(false), queue.releaseCounted)
    }

    @Test
    fun `server error is retried and counted`() = runBlocking {
        val queue = FakeQueue(listOf(chapterDelta(1)))
        val remote = FakeRemote(onPush = { throw HttpException(503) })
        val outcome = DeltaSyncPushEngine(queue, remote, preferences(), protoBuf).run()

        assertTrue(outcome is DeltaSyncPushEngine.Outcome.Retry)
        assertEquals(listOf(1L), queue.released)
        assertEquals(listOf(true), queue.releaseCounted)
        assertTrue(queue.parked.isEmpty())
    }

    @Test
    fun `one rejected row does not hold back the rest of the batch`() = runBlocking {
        val queue = FakeQueue(listOf(chapterDelta(1), chapterDelta(2, url = "/bad"), chapterDelta(3), chapterDelta(4)))
        val remote = FakeRemote(onPush = { bytes ->
            val request = ProtoBuf.decodeFromByteArray(DeltaPushRequest.serializer(), bytes)
            if (request.chapters.any { it.chapter.url == "/bad" }) throw HttpException(422)
        })
        val outcome = DeltaSyncPushEngine(queue, remote, preferences(), protoBuf).run()

        assertEquals(DeltaSyncPushEngine.Outcome.Completed(uploaded = 3, parked = 1), outcome)
        assertEquals(listOf(2L), queue.parked)
        assertEquals(setOf(1L, 3L, 4L), queue.acked.toSet())
        assertTrue(queue.released.isEmpty())
    }

    @Test
    fun `disabled sync does nothing`() = runBlocking {
        val queue = FakeQueue(listOf(chapterDelta(1)))
        val remote = FakeRemote()
        val outcome = DeltaSyncPushEngine(queue, remote, preferences(enabled = false), protoBuf).run()

        assertEquals(DeltaSyncPushEngine.Outcome.Disabled, outcome)
        assertTrue(remote.pushes.isEmpty())
        assertTrue(queue.acked.isEmpty())
    }

    private class FakeRecords(private val failOn: String? = null) : SyncRecordRepository {
        val chapters = mutableListOf<RemoteChapterDelta>()
        var staleFlagsCleared = 0

        override suspend fun applyRemoteManga(delta: RemoteMangaDelta) = RemoteApplyResult.UPDATED

        override suspend fun applyRemoteChapter(delta: RemoteChapterDelta): RemoteApplyResult {
            if (delta.chapter.url == failOn) error("constraint failed")
            chapters += delta
            return RemoteApplyResult.UPDATED
        }

        override suspend fun clearStaleSyncFlags() {
            staleFlagsCleared++
        }
    }

    private fun wireChapter(url: String) = ChapterDelta(7, "/manga", BackupChapter(url = url, name = url, read = true))

    @Test
    fun `pull applies deltas and only then stores the cursor`() = runBlocking {
        val cursor = MemoryPreference("")
        val records = FakeRecords()
        val remote = FakeRemote(onPull = { since ->
            if (since.isEmpty()) {
                DeltaPullResponse(chapters = listOf(wireChapter("/a"), wireChapter("/b")), cursor = "42")
            } else {
                DeltaPullResponse()
            }
        })
        val outcome = DeltaSyncPullEngine(remote, records, preferences(cursor = cursor)).run()

        assertEquals(DeltaSyncPullEngine.Outcome.Completed(applied = 2, skipped = 0, dropped = 0), outcome)
        assertEquals(listOf("/a", "/b"), records.chapters.map { it.chapter.url })
        assertEquals("42", cursor.value)
        assertEquals(1, records.staleFlagsCleared)
    }

    @Test
    fun `pull keeps the cursor when a record cannot be applied`() = runBlocking {
        val cursor = MemoryPreference("7")
        val records = FakeRecords(failOn = "/b")
        val remote = FakeRemote(onPull = {
            DeltaPullResponse(chapters = listOf(wireChapter("/a"), wireChapter("/b")), cursor = "8")
        })
        val outcome = DeltaSyncPullEngine(remote, records, preferences(cursor = cursor)).run(runAttempt = 0)

        assertTrue(outcome is DeltaSyncPullEngine.Outcome.Retry)
        assertEquals("7", cursor.value)
    }

    @Test
    fun `pull gives up on a poisonous record after repeated attempts`() = runBlocking {
        val cursor = MemoryPreference("7")
        val records = FakeRecords(failOn = "/b")
        val remote = FakeRemote(onPull = { since ->
            if (since == "7") {
                DeltaPullResponse(chapters = listOf(wireChapter("/a"), wireChapter("/b")), cursor = "8")
            } else {
                DeltaPullResponse()
            }
        })
        val outcome = DeltaSyncPullEngine(remote, records, preferences(cursor = cursor))
            .run(runAttempt = DeltaSyncPullEngine.MAX_RECORD_ATTEMPTS - 1)

        assertEquals(DeltaSyncPullEngine.Outcome.Completed(applied = 1, skipped = 0, dropped = 1), outcome)
        assertEquals("8", cursor.value)
    }
}
