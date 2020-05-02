package ru.gluck1986.unique.ip.utils

import java.io.BufferedReader
import java.io.File
import java.net.Inet4Address
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.concurrent.fixedRateTimer

class UniqueCounter(private val maxThreads: Int = 1) {

    private var progressHandler: ((totalBytes: Long, currentBytesRead: Long) -> Unit) = { _, _ -> }
    private var onFinish: ((rows: Long, unique: Long, Collection<String>) -> Unit) = { _, _, _ -> }
    private val executorService: ExecutorService = Executors.newFixedThreadPool(maxThreads)

    fun setProgressHandler(cb: (totalBytes: Long, currentBytesRead: Long) -> Unit) {
        progressHandler = cb
    }

    fun setOnFinish(cb: (rows: Long, unique: Long, Collection<String>) -> Unit) {
        onFinish = cb
    }

    fun count(filePath: String) {
        val state = State(maxThreads, filePath)
        val reader = Files.newBufferedReader(Path.of(filePath))

        val progressTimer = fixedRateTimer("progress_tick", false, 500L, 500L) {
            progressHandler(state.fileSize, state.currentSize)
        }

        val executorRead = Executors.newSingleThreadExecutor()
        val jobRead = executorRead.submit(prepareReadJob(reader, state))

        val jobPerform = (0 until maxThreads).map {
            executorService.submit((preparePerformJob(it, state)))
        }
        jobRead.get()
        jobPerform.forEach {
            it.get()
        }
        executorRead.shutdown()
        executorService.shutdown()
        progressTimer.cancel()
        onFinish(state.totalRows, state.totalUniquesCollection.sum(), state.errors)
    }

    private fun prepareReadJob(reader: BufferedReader, state: State): () -> Unit {
        return {
            var line: String?
            var bufferIterator = 0
            while (reader.readLine().also { line = it } != null) {
                state.totalRows++
                state.currentSize += (line!!.length + Char.SIZE_BYTES) //add line feed weight
                state.buffers[bufferIterator].put(line!!)
                bufferIterator++
                if (bufferIterator >= maxThreads) {
                    bufferIterator = 0
                }
            }
            for (finalIterator in 0 until maxThreads) {
                state.buffers[finalIterator].put(DONE)
            }
        }
    }

    private fun preparePerformJob(bufferNumber: Int, state: State): Callable<Unit> {
        return Callable {
            var line: String?
            while (state.buffers[bufferNumber].take().also { line = it } != DONE) {
                if (!isExistsOrLogError(line!!, state.uniqueSet, state.errors)) {
                    state.totalUniquesCollection[bufferNumber]++
                }
            }
        }
    }

    private fun isExistsOrLogError(it: String, uniqueSet: BitArrayLong, errors: MutableCollection<String>): Boolean {
        try {
            val address = Inet4Address.getByAddress(parse(it))
            val longAddress: Long = (ByteBuffer.wrap(address.address).int.toLong() and 0xffffffffL)
            synchronized(uniqueSet) {
                if (!uniqueSet.getBit(longAddress)) {
                    uniqueSet.setBit(longAddress, true)
                    return false
                }
            }
        } catch (e: Exception) {
            errors.add(it)
        }
        return true
    }

    private fun parse(address: String): ByteArray {
        val ip = ByteArray(4)
        val parts = address.split("\\.".toRegex()).toTypedArray()

        for (i in 0..3) {
            ip[i] = parts[i].toInt().toByte()
        }
        return ip
    }

    class State(maxThreads: Int, filePath: String) {
        val totalUniquesCollection: Array<Long> = Array(maxThreads) { 0L }
        var totalRows: Long = 0
        val errors = arrayListOf<String>()
        var currentSize = 0L
        val uniqueSet = BitArrayLong(MAX_IP)
        val buffers = Array(maxThreads) {
            ArrayBlockingQueue<String>(MAX_LINES)
        }
        val fileSize = File(filePath).length()
    }

    companion object {
        private const val MAX_IP = 4294967295L
        private const val MAX_LINES = 10000
        private const val DONE = "DONE"
    }
}

