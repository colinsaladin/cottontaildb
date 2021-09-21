package org.vitrivr.cottontail.storage.io

import jetbrains.exodus.ExodusException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.*
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicInteger

/**
 * A [SharedFileChannel] instance, which is basically a wrapper around a [FileChannel] with reference counting.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SharedFileChannel constructor(path: Path): AutoCloseable {

    init {
        if (!Files.isWritable(path)) {
            path.toFile().setWritable(true)
        }
    }

    /** Internal [FileChannel] reference. */
    private val wrapped: FileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)

    /** Counter for the number of users of this [SharedFileChannel]. */
    private val refCounter: AtomicInteger = AtomicInteger()

    /**
     * Returns the size of this [SharedFileChannel].
     */
    fun size(): Long = this.wrapped.size()

    /**
     * Initializes this [SharedFileChannel] with the given [length].
     *
     * @param length The length to initialize this [SharedFileChannel] with.
     */
    fun ensureLength(length: Long) {
        if (this.wrapped.size() < length) {
            this.wrapped.write(ByteBuffer.allocate(1), length)
        }
    }

    /**
     * Truncates this [SharedFileChannel] to the given [length].
     *
     * @param length The new length.
     */
    fun truncate(length: Long) {
        this.wrapped.truncate(length)
    }

    /**
     * Reads [count] bytes from the [FileChannel]'s [position] to [offset] in the [output] [ByteArray]
     *
     * @param output The output [ByteArray]
     * @param position The [position] in the [FileChannel] to read from.
     * @param offset The offset in the [ByteArray] to read to.
     * @param count The number of bytes to read.
     * @return The effective number of bytes that were read.
     */
    fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int {
        val inBuffer = ByteBuffer.wrap(output).position(offset).limit(offset + count)
        return this.wrapped.read(inBuffer, position)
    }

    /**
     * Writes [count] bytes from the provided [ByteArray] to the [FileChannel] starting at position [offset].
     *
     * @param position The position in the file to write to.
     * @param input The [ByteArray] to read data from.
     * @param offset The offset into the [ByteArray].
     * @param count The number of bytes to write.
     * @return The effective number of bytes that were read.
     */
    fun write(input: ByteArray, position: Long, offset: Int, count: Int): Int {
        val outBuffer = ByteBuffer.wrap(input).position(offset).limit(offset + count)
        return this.wrapped.write(outBuffer, position)
    }

    /**
     * Tries to force updates made through this [FileChannel] to disk.
     */
    fun force() {
        try {
            this.wrapped.force(false)
        } catch (e: ClosedChannelException) {
        } catch (ioe: IOException) {
            if (this.wrapped.isOpen) {
                throw ExodusException(ioe)
            }
        }
    }

    /**
     * Retains this [SharedFileChannel], increasing its [refCounter] by one.
     *
     * @return New reference counter value.
     */
    fun retain(): Int = this.refCounter.incrementAndGet()

    /**
     * Closes this [SharedFileChannel].
     *
     * Decreases the [refCounter] until it drops to zero, then closes the underlying [FileChannel]
     */
    override fun close() {
        if (this.refCounter.getAndDecrement() == 0) {
            this.wrapped.close()
        }
    }
}