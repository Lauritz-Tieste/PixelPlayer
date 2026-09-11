package com.theveloper.pixelplay.utils

import com.theveloper.pixelplay.data.model.Song
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.TimeUnit
import kotlin.random.Random

class QueueUtilsTest {

    @Test
    fun buildAnchoredShuffleQueueSuspending_handles10kSongsWithoutLosingItems() = runBlocking {
        val songs = buildSongs(10_000)
        val anchorIndex = 7_654

        val shuffled = withTimeout(5_000L) {
            QueueUtils.buildAnchoredShuffleQueueSuspending(
                currentQueue = songs,
                anchorIndex = anchorIndex,
                random = Random(42)
            )
        }

        assertEquals("Queue size must stay the same", songs.size, shuffled.size)
        assertEquals(
            "Current song must stay anchored so playback is not redirected",
            songs[anchorIndex].id,
            shuffled[anchorIndex].id
        )

        val originalIds = songs.map { it.id }.toSet()
        val shuffledIds = shuffled.map { it.id }.toSet()
        assertEquals("Shuffled queue must contain the same songs", originalIds, shuffledIds)
    }

    @Test
    fun buildAnchoredShuffleQueueSuspending_yieldsForLargeQueues() = runBlocking {
        val songs = buildSongs(10_000)
        val started = CompletableDeferred<Unit>()
        var heartbeat = 0

        val heartbeatJob = launch {
            started.complete(Unit)
            while (true) {
                heartbeat++
                yield()
            }
        }

        started.await()
        yield() // Let heartbeat run at least once before measuring.
        val beforeShuffleHeartbeat = heartbeat

        withTimeout(5_000L) {
            QueueUtils.buildAnchoredShuffleQueueSuspending(
                currentQueue = songs,
                anchorIndex = 4_321,
                random = Random(7)
            )
        }

        val afterShuffleHeartbeat = heartbeat
        heartbeatJob.cancelAndJoin()

        assertTrue(
            "Shuffle should yield cooperatively so sibling coroutines can run",
            afterShuffleHeartbeat > beforeShuffleHeartbeat
        )
    }

    @Test
    fun buildAnchoredShuffleQueueSuspending_startAtZero_placesAnchorFirst() = runBlocking {
        val songs = buildSongs(32)
        val anchorIndex = 11

        val shuffled = QueueUtils.buildAnchoredShuffleQueueSuspending(
            currentQueue = songs,
            anchorIndex = anchorIndex,
            startAtZero = true,
            random = Random(99)
        )

        assertEquals("Anchor song must become the first item", songs[anchorIndex].id, shuffled.first().id)
        assertEquals("Queue size must stay the same", songs.size, shuffled.size)
        assertEquals(
            "Shuffled queue must contain the same songs",
            songs.map { it.id }.toSet(),
            shuffled.map { it.id }.toSet()
        )
    }

    @Test
    fun buildAnchoredShuffleQueueSuspending_balancesFavoritesWithDiscovery() = runBlocking {
        val now = System.currentTimeMillis()
        val songs = (0 until 30).map { index ->
            val isFavorite = index < 20
            val dateAdded = if (!isFavorite && index % 2 == 0) now else now - TimeUnit.DAYS.toMillis(120)
            buildSong(
                index = index,
                isFavorite = isFavorite,
                dateAdded = dateAdded,
                artistId = (index % 6).toLong() + 1L
            )
        }

        val shuffled = QueueUtils.buildAnchoredShuffleQueueSuspending(
            currentQueue = songs,
            anchorIndex = 0,
            startAtZero = true,
            random = Random(123)
        )

        val firstWindow = shuffled.drop(1).take(14)
        val nonFavoriteCount = firstWindow.count { !it.isFavorite }
        val discoveryCount = firstWindow.count { now - normalizeDateAdded(it.dateAdded) <= TimeUnit.DAYS.toMillis(45) }

        assertTrue("Early window should include non-favorites for variety", nonFavoriteCount >= 5)
        assertTrue("Early window should include recently added discovery songs", discoveryCount >= 3)
    }

    private fun buildSongs(count: Int): List<Song> = List(count) { index ->
        buildSong(index)
    }

    private fun buildSong(
        index: Int,
        isFavorite: Boolean = false,
        dateAdded: Long = 0L,
        artistId: Long = 1L
    ) = Song(
        id = "song-$index",
        title = "Song $index",
        artist = "Artist",
        artistId = artistId,
        album = "Album",
        albumId = 1L,
        path = "/tmp/song-$index.mp3",
        contentUriString = "content://pixelplay/song/$index",
        albumArtUriString = null,
        duration = 180_000L,
        isFavorite = isFavorite,
        dateAdded = dateAdded,
        mimeType = "audio/mpeg",
        bitrate = 320_000,
        sampleRate = 44_100
    )

    private fun normalizeDateAdded(dateAdded: Long): Long {
        return when {
            dateAdded <= 0L -> 0L
            dateAdded < 1_000_000_000_000L -> dateAdded * 1000L
            else -> dateAdded
        }
    }
}
