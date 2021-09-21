package org.vitrivr.cottontail.storage.io

import jetbrains.exodus.io.*
import jetbrains.exodus.ExodusException
import jetbrains.exodus.OutOfDiskSpaceException
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.system.JVMConstants
import mu.KLogging
import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name

/**
 * A [DataWriter] implementation that uses [SharedFileChannel]s to access files.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FileChannelDataWriter constructor(private val reader: FileChannelDataReader, lockId: String? = null) : AbstractDataWriter() {

    companion object : KLogging() {
        private const val DELETED_FILE_EXTENSION = ".del"
    }

    /** The [FileChannel] for the underlying directory. */
    private var dirChannel: FileChannel = FileChannel.open(this.reader.path)

    /** Internal [LockingManager] instance. */
    private val lockingManager: LockingManager = LockingManager(this.reader.path.toFile(), lockId)

    /** [SharedFileChannel] instance that is currently being written. Corresponds with [block].*/
    private var channel: SharedFileChannel? = null

    /** [FileChannelDataReader.FileChannelBlock] instance that is currently being written. Corresponds with [channel]. */
    private var block: FileChannelDataReader.FileChannelBlock? = null

    init {
        if (!JVMConstants.IS_ANDROID) {
            this.syncDirectory()
        }
    }

    /**
     * Writes [count] bytes from the provided [ByteArray] to the [SharedFileChannel] starting at position [offset].
     *
     * @param input The [ByteArray] to read data from.
     * @param offset The offset into the [ByteArray].
     * @param count The number of bytes to write.
     * @return The effective number of bytes that were read.
     */
    override fun write(input: ByteArray, offset: Int, count: Int): Block {
        try {
            (this.channel ?: throw ExodusException("Can't write, FileDataWriter is closed")).write(input, offset, count)
        } catch (ioe: IOException) {
            if (this.lockingManager.usableSpace < count) {
                throw OutOfDiskSpaceException(ioe)
            }
            throw ExodusException("Can't write", ioe)
        }
        return this.block ?: throw ExodusException("Can't write, FileDataWriter is closed")
    }

    /**
     *
     */
    override fun lock(timeout: Long): Boolean {
        return this.lockingManager.lock(timeout)
    }

    /**
     *
     */
    override fun release(): Boolean {
        return this.lockingManager.release()
    }

    /**
     *
     */
    override fun lockInfo(): String? {
        return this.lockingManager.lockInfo()
    }

    /**
     * Synchronizes the currently open [Block] [FileChannelDataWriter].
     */
    override fun syncImpl() {
        this.channel?.force()
    }

    /**
     * Closes this [FileChannelDataWriter].
     */
    override fun closeImpl() {
        try {
            (this.channel ?: throw ExodusException("Can't close already closed FileDataWriter")).close()
            this.channel = null
            this.block = null
            this.dirChannel.close()
        } catch (e: IOException) {
            throw ExodusException("Failed to close FileChannelDataWriter", e)
        }
    }

    /**
     * Clears all files associated with the directory this [FileChannelDataWriter] belongs to.
     */
    override fun clearImpl() {
        Files.newDirectoryStream(this.reader.path).filter {
            val filename = it.fileName.toString()
            filename.length == LogUtil.LOG_FILE_NAME_WITH_EXT_LENGTH && filename.endsWith(LogUtil.LOG_FILE_EXTENSION)
        }.forEach {
            try {
                Files.delete(it)
            } catch (e: IOException) {
                throw ExodusException("Failed to delete $it")
            }
        }
    }

    /**
     * Opens or creates a new [Block] for the given [address] and [length].
     *
     * @param address The address of the desired [Block].
     * @param length The length of the desired [Block].
     */
    override fun openOrCreateBlockImpl(address: Long, length: Long): Block {
        try {
            /* Create block and initialize it, if needed. */
            val result = this.reader.FileChannelBlock(address)
            val channel = result.channel()
            if (channel.size() == 0L) {
                channel.init(length)
            }

            /* Update local pointers. */
            this.channel = channel
            this.block = result
            return result
        } catch (ioe: IOException) {
            throw ExodusException(ioe)
        }
    }

    /**
     * Removes / deletes the [Block] for the given [blockAddress].
     *
     * @param blockAddress The address of the [Block] to remove.
     * @param rbt The [RemoveBlockType]
     */
    override fun removeBlock(blockAddress: Long, rbt: RemoveBlockType) {
        val block = this.reader.FileChannelBlock(blockAddress)
        removeFileFromFileCache(block.path)
        try {
            if (rbt == RemoveBlockType.Delete) {
                Files.delete(block.path)
            } else {
                Files.move(block.path, block.path.parent.resolve(block.path.name.substring(0, block.path.name.indexOf(LogUtil.LOG_FILE_EXTENSION)) + DELETED_FILE_EXTENSION))
            }
            if (FileDataReader.logger.isInfoEnabled) {
                FileDataReader.logger.info("Deleted file ${block.path}.")
            }
        } catch (e: IOException) {
            throw ExodusException("Failed to delete file ${block.path}.")
        }
    }

    /**
     * Truncates the [Block] for the given [blockAddress].
     *
     * @param blockAddress The address of the [Block] to remove.
     * @param length The desired length of the [Block]
     */
    override fun truncateBlock(blockAddress: Long, length: Long) {
        val block = this.reader.FileChannelBlock(blockAddress)
        removeFileFromFileCache(block.path) /* Invalidate cached channel. */
        try {
            block.channel().truncate(length)
            if (FileDataReader.logger.isInfoEnabled) {
                FileDataReader.logger.info("Truncated file ${block.path} to length $length")
            }
        } catch (e: IOException) {
            throw ExodusException("Failed to truncate file ${block.path}.")
        }
    }

    /**
     * Synchronizes the directory this [FileChannelDataWriter] belongs to with the underlying file system.
     */
    override fun syncDirectory() {
        if (dirChannel.isOpen) {
            try {
                this.dirChannel.force(false)
            } catch (e: IOException) {
                logger.warn("Failed to synchronize directory!")
            }
        }
    }

    /**
     * Removes the [SharedFileChannel] for the given [Path] from the [SharedFileChannelCache].
     *
     * @param path The [Path] of the [SharedFileChannel] to remove.
     */
    private fun removeFileFromFileCache(path: Path) {
        try {
            SharedFileChannelCache.removeFile(path)
        } catch (e: IOException) {
            throw ExodusException(e)
        }
    }
}
