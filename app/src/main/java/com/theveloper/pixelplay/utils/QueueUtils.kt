package com.theveloper.pixelplay.utils

import com.theveloper.pixelplay.data.model.Song
import kotlinx.coroutines.yield
import kotlin.random.Random
import kotlin.math.roundToInt

object QueueUtils {

    /**
     * Batch size for yielding during shuffle of very large collections.
     * Every [SHUFFLE_YIELD_BATCH] swaps we yield to avoid blocking the caller coroutine.
     */
    private const val SHUFFLE_YIELD_BATCH = 512
    private const val MIN_BALANCED_POOL_SIZE = 8
    private const val FAVORITE_TARGET_RATIO = 0.35
    private const val FAVORITE_MIN_RATIO = 0.20
    private const val FAVORITE_MAX_RATIO = 0.50
    private const val DISCOVERY_SLOT_INTERVAL = 3
    private const val NEW_SONG_WINDOW_MS = 45L * 24L * 60L * 60L * 1000L

    fun <T> fisherYatesCopy(source: List<T>, random: Random = Random.Default): List<T> {
        if (source.size <= 1) return source.toList()
        val mutable = source.toMutableList()
        for (i in mutable.lastIndex downTo 1) {
            val j = random.nextInt(i + 1)
            if (i != j) {
                val tmp = mutable[i]
                mutable[i] = mutable[j]
                mutable[j] = tmp
            }
        }
        return mutable
    }

    private fun generateShuffleOrder(
        currentQueue: List<Song>,
        anchorIndex: Int,
        random: Random = Random.Default
    ): IntArray {
        val size = currentQueue.size
        if (size <= 1) return IntArray(size) { it }

        val clampedAnchor = anchorIndex.coerceIn(0, size - 1)
        val pool = buildBalancedPool(currentQueue, clampedAnchor, random)

        val order = IntArray(size)
        var poolIndex = 0
        for (i in 0 until size) {
            order[i] = if (i == clampedAnchor) clampedAnchor else pool[poolIndex++]
        }
        return order
    }

    /**
     * Suspendable version of [generateShuffleOrder] that yields periodically for large queues.
     * This prevents ANR when shuffling 10,000+ songs by cooperating with the coroutine dispatcher.
     */
    private suspend fun generateShuffleOrderSuspending(
        currentQueue: List<Song>,
        anchorIndex: Int,
        random: Random = Random.Default
    ): IntArray {
        val size = currentQueue.size
        if (size <= 1) return IntArray(size) { it }

        val clampedAnchor = anchorIndex.coerceIn(0, size - 1)
        val pool = buildBalancedPoolSuspending(currentQueue, clampedAnchor, random)

        val order = IntArray(size)
        var poolIndex = 0
        var workSinceYield = 0
        for (i in 0 until size) {
            order[i] = if (i == clampedAnchor) clampedAnchor else pool[poolIndex++]
            workSinceYield++
            if (workSinceYield >= SHUFFLE_YIELD_BATCH) {
                workSinceYield = 0
                yield()
            }
        }
        return order
    }

    fun buildAnchoredShuffleQueue(
        currentQueue: List<Song>,
        anchorIndex: Int,
        random: Random = Random.Default
    ): List<Song> {
        if (currentQueue.size <= 1) return currentQueue.toList()
        val order = generateShuffleOrder(currentQueue, anchorIndex, random)
        return List(order.size) { idx -> currentQueue[order[idx]] }
    }

    /**
     * Suspendable shuffle that yields periodically for large queues (10,000+).
     * Maintains O(n) Fisher-Yates complexity with uniform randomness.
     */
    suspend fun buildAnchoredShuffleQueueSuspending(
        currentQueue: List<Song>,
        anchorIndex: Int,
        startAtZero: Boolean = false,
        random: Random = Random.Default
    ): List<Song> {
        if (currentQueue.size <= 1) return currentQueue.toList()
        
        val order = if (startAtZero) {
             generateShuffleOrderStartAtZero(currentQueue, anchorIndex, random)
        } else {
             generateShuffleOrderSuspending(currentQueue, anchorIndex, random)
        }
        return List(order.size) { idx -> currentQueue[order[idx]] }
    }

    private suspend fun generateShuffleOrderStartAtZero(
        currentQueue: List<Song>,
        anchorIndex: Int,
        random: Random = Random.Default
    ): IntArray {
        val size = currentQueue.size
        if (size <= 1) return IntArray(size) { it }
        val clampedAnchor = anchorIndex.coerceIn(0, size - 1)
        val pool = buildBalancedPoolSuspending(currentQueue, clampedAnchor, random)
        var workSinceYield = 0

        // Construct final order: Anchor is ALWAYS at 0, followed by shuffled pool
        val order = IntArray(size)
        order[0] = clampedAnchor
        
        for (i in 0 until pool.size) {
            order[i + 1] = pool[i]
             workSinceYield++
            if (workSinceYield >= SHUFFLE_YIELD_BATCH) {
                workSinceYield = 0
                yield()
            }
        }
        
        return order
    }

    private fun buildBalancedPool(currentQueue: List<Song>, anchorIndex: Int, random: Random): IntArray {
        val size = currentQueue.size
        val allIndices = IntArray(size - 1)
        var cursor = 0
        for (i in 0 until size) {
            if (i != anchorIndex) allIndices[cursor++] = i
        }

        if (allIndices.size < MIN_BALANCED_POOL_SIZE) {
            shuffleIntArrayInPlace(allIndices, random)
            return allIndices
        }

        val nowMs = System.currentTimeMillis()
        val favoriteQueue = ArrayDeque<Int>(allIndices.size)
        val discoveryQueue = ArrayDeque<Int>(allIndices.size)
        val regularQueue = ArrayDeque<Int>(allIndices.size)

        allIndices.forEach { index ->
            val song = currentQueue[index]
            when {
                song.isFavorite -> favoriteQueue.addLast(index)
                isDiscoveryCandidate(song, nowMs) -> discoveryQueue.addLast(index)
                else -> regularQueue.addLast(index)
            }
        }

        shuffleDequeInPlace(favoriteQueue, random)
        shuffleDequeInPlace(discoveryQueue, random)
        shuffleDequeInPlace(regularQueue, random)

        val nonFavoritePool = buildNonFavoritePool(discoveryQueue, regularQueue)
        val targetFavoriteCount = computeFavoriteTarget(
            totalCount = allIndices.size,
            favoriteCount = favoriteQueue.size,
            nonFavoriteCount = nonFavoritePool.size
        )
        val balancedPool = mergeFavoritesAndNonFavorites(
            favoriteQueue = favoriteQueue,
            nonFavoritePool = nonFavoritePool,
            targetFavoriteCount = targetFavoriteCount
        )
        reduceArtistClumps(currentQueue, balancedPool)
        return balancedPool.toIntArray()
    }

    private suspend fun buildBalancedPoolSuspending(
        currentQueue: List<Song>,
        anchorIndex: Int,
        random: Random
    ): IntArray {
        val size = currentQueue.size
        val allIndices = IntArray(size - 1)
        var cursor = 0
        var workSinceYield = 0
        for (i in 0 until size) {
            if (i != anchorIndex) allIndices[cursor++] = i
            workSinceYield++
            if (workSinceYield >= SHUFFLE_YIELD_BATCH) {
                workSinceYield = 0
                yield()
            }
        }

        if (allIndices.size < MIN_BALANCED_POOL_SIZE) {
            shuffleIntArrayInPlace(allIndices, random)
            return allIndices
        }

        val nowMs = System.currentTimeMillis()
        val favoriteQueue = ArrayDeque<Int>(allIndices.size)
        val discoveryQueue = ArrayDeque<Int>(allIndices.size)
        val regularQueue = ArrayDeque<Int>(allIndices.size)

        allIndices.forEach { index ->
            val song = currentQueue[index]
            when {
                song.isFavorite -> favoriteQueue.addLast(index)
                isDiscoveryCandidate(song, nowMs) -> discoveryQueue.addLast(index)
                else -> regularQueue.addLast(index)
            }
            workSinceYield++
            if (workSinceYield >= SHUFFLE_YIELD_BATCH) {
                workSinceYield = 0
                yield()
            }
        }

        shuffleDequeInPlace(favoriteQueue, random)
        shuffleDequeInPlace(discoveryQueue, random)
        shuffleDequeInPlace(regularQueue, random)
        yield()

        val nonFavoritePool = buildNonFavoritePool(discoveryQueue, regularQueue)
        val targetFavoriteCount = computeFavoriteTarget(
            totalCount = allIndices.size,
            favoriteCount = favoriteQueue.size,
            nonFavoriteCount = nonFavoritePool.size
        )
        val balancedPool = mergeFavoritesAndNonFavorites(
            favoriteQueue = favoriteQueue,
            nonFavoritePool = nonFavoritePool,
            targetFavoriteCount = targetFavoriteCount
        )
        reduceArtistClumps(currentQueue, balancedPool)
        return balancedPool.toIntArray()
    }

    private fun isDiscoveryCandidate(song: Song, nowMs: Long): Boolean {
        val normalizedDateAddedMs = when {
            song.dateAdded <= 0L -> 0L
            song.dateAdded < 1_000_000_000_000L -> song.dateAdded * 1000L
            else -> song.dateAdded
        }
        return normalizedDateAddedMs > 0L && (nowMs - normalizedDateAddedMs) <= NEW_SONG_WINDOW_MS
    }

    private fun buildNonFavoritePool(
        discoveryQueue: ArrayDeque<Int>,
        regularQueue: ArrayDeque<Int>
    ): MutableList<Int> {
        val result = ArrayList<Int>(discoveryQueue.size + regularQueue.size)
        var slot = 0
        while (discoveryQueue.isNotEmpty() || regularQueue.isNotEmpty()) {
            val takeDiscovery = discoveryQueue.isNotEmpty() &&
                (slot % DISCOVERY_SLOT_INTERVAL == 0 || regularQueue.isEmpty())
            if (takeDiscovery) {
                result.add(discoveryQueue.removeFirst())
            } else if (regularQueue.isNotEmpty()) {
                result.add(regularQueue.removeFirst())
            } else if (discoveryQueue.isNotEmpty()) {
                result.add(discoveryQueue.removeFirst())
            }
            slot++
        }
        return result
    }

    private fun computeFavoriteTarget(totalCount: Int, favoriteCount: Int, nonFavoriteCount: Int): Int {
        if (totalCount <= 0 || favoriteCount <= 0) return 0
        if (nonFavoriteCount <= 0) return favoriteCount
        val baseline = (totalCount * FAVORITE_TARGET_RATIO).roundToInt()
        val minCap = (totalCount * FAVORITE_MIN_RATIO).roundToInt().coerceAtLeast(1)
        val maxCap = (totalCount * FAVORITE_MAX_RATIO).roundToInt().coerceAtLeast(minCap)
        val clamped = baseline.coerceIn(minCap, maxCap)
        return clamped.coerceAtMost(favoriteCount).coerceAtMost(totalCount - 1)
    }

    private fun mergeFavoritesAndNonFavorites(
        favoriteQueue: ArrayDeque<Int>,
        nonFavoritePool: List<Int>,
        targetFavoriteCount: Int
    ): MutableList<Int> {
        if (favoriteQueue.isEmpty()) return nonFavoritePool.toMutableList()
        if (nonFavoritePool.isEmpty()) return favoriteQueue.toMutableList()

        val output = ArrayList<Int>(favoriteQueue.size + nonFavoritePool.size)
        val nonFavorites = ArrayDeque(nonFavoritePool)
        val desiredFavorites = targetFavoriteCount.coerceAtMost(favoriteQueue.size)
        val insertionGap = ((nonFavoritePool.size + desiredFavorites).toDouble() / (desiredFavorites + 1)).coerceAtLeast(1.0)
        var insertedFavorites = 0
        var nextFavoriteAt = insertionGap

        while (favoriteQueue.isNotEmpty() || nonFavorites.isNotEmpty()) {
            val shouldInsertFavorite =
                favoriteQueue.isNotEmpty() &&
                    insertedFavorites < desiredFavorites &&
                    (output.size + 1 >= nextFavoriteAt.roundToInt() || nonFavorites.isEmpty())

            if (shouldInsertFavorite) {
                output.add(favoriteQueue.removeFirst())
                insertedFavorites++
                nextFavoriteAt += insertionGap
                continue
            }

            if (nonFavorites.isNotEmpty()) {
                output.add(nonFavorites.removeFirst())
            } else if (favoriteQueue.isNotEmpty()) {
                output.add(favoriteQueue.removeFirst())
            }
        }

        return output
    }

    private fun reduceArtistClumps(currentQueue: List<Song>, indices: MutableList<Int>) {
        if (indices.size < 3) return
        if (indices.map { currentQueue[it].artistId }.distinct().size <= 1) return
        for (i in 1 until indices.size) {
            val previous = currentQueue[indices[i - 1]]
            val current = currentQueue[indices[i]]
            if (previous.artistId != current.artistId) continue

            val swapIndex = ((i + 1)..indices.lastIndex)
                .firstOrNull { candidateIndex ->
                    currentQueue[indices[candidateIndex]].artistId != previous.artistId
                }
            if (swapIndex != null) {
                val temp = indices[i]
                indices[i] = indices[swapIndex]
                indices[swapIndex] = temp
            }
        }
    }

    private fun shuffleDequeInPlace(queue: ArrayDeque<Int>, random: Random) {
        if (queue.size <= 1) return
        val temp = queue.toMutableList()
        shuffleMutableListInPlace(temp, random)
        queue.clear()
        temp.forEach(queue::addLast)
    }

    private fun shuffleMutableListInPlace(list: MutableList<Int>, random: Random) {
        for (i in list.lastIndex downTo 1) {
            val j = random.nextInt(i + 1)
            if (i != j) {
                val tmp = list[i]
                list[i] = list[j]
                list[j] = tmp
            }
        }
    }

    private fun shuffleIntArrayInPlace(array: IntArray, random: Random) {
        for (i in array.lastIndex downTo 1) {
            val j = random.nextInt(i + 1)
            if (i != j) {
                val tmp = array[i]
                array[i] = array[j]
                array[j] = tmp
            }
        }
    }

}
