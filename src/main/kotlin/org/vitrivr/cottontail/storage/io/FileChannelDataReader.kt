package org.vitrivr.cottontail.storage.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.io.*
import jetbrains.exodus.log.Log
import jetbrains.exodus.log.LogUtil
import jetbrains.exodus.log.LogUtil.LOG_FILE_EXTENSION
import mu.KLogging
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * A [DataReader] implementation that uses [SharedFileChannel]s to access files.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FileChannelDataReader(val path: Path): DataReader, KLogging() {

    /** The [Log] reference used by this [FileChannelDataReader] */
     var log: Log? = null
        internal set

    /**
     * Returns the location of this [FileChannelDataReader]'s directory as [String].
     *
     * @return [String] representation of the [Path] to this [FileChannelDataReader]'s directory.
     */
    override fun getLocation(): String = this.path.toString()

    /**
     * Returns all Log blocks sorted by address.
     *
     * @return [FileChannelBlock] sorted by address
     */
    override fun getBlocks(): Iterable<Block> = Files.newDirectoryStream(this.path).asSequence().filter {
        it.fileName.toString().endsWith(LOG_FILE_EXTENSION)
    }.map {
        FileChannelBlock(LogUtil.getAddress(it.fileName.toString()))
    }.sortedBy {
        it.address
    }.asIterable()

    /**
     * Returns [FileChannelBlock] sorted by address with address greater than or equal to specified fromAddress.
     *
     * @param fromAddress Starting block address
     * @return [FileChannelBlock] sorted by address
     */
    override fun getBlocks(fromAddress: Long): Iterable<Block> = Files.newDirectoryStream(this.path).asSequence().filter {
        it.fileName.toString().endsWith(LOG_FILE_EXTENSION) && LogUtil.getAddress(it.fileName.toString()) >= fromAddress
    }.map {
        FileChannelBlock(LogUtil.getAddress(it.fileName.toString()))
    }.sortedBy {
        it.address
    }.asIterable()

    /**
     * Closes this [FileChannelDataReader].
     */
    override fun close() {
        try {
            SharedFileChannelCache.removeDirectory(this.path)
        } catch (e: IOException) {
            throw ExodusException("Could not close all files", e)
        }
    }

    /**
     * A [Block] implementation based on [SharedFileChannel]s.
     */
    inner class FileChannelBlock(private val address: Long): Block {

        /** Path to the file that represents this [FileChannelBlock]. */
        val path: Path
            get() = this@FileChannelDataReader.path.resolve(LogUtil.getLogFilename(this.address))

        /**
         * Returns address of this [FileChannelBlock]] in the Log. This address is always constant for the block.
         */
        override fun getAddress() = this.address

        /**
         * Returns the size in bytes of this [FileChannelBlock].
         */
        override fun length(): Long = Files.size(this.path)

        /**
         * Reads data from the underlying store device.
         *
         * @param output Array to copy data to
         * @param offset Starting offset in the array
         * @param count Number of bytes to read.
         * @return Number of bytes read.
         */
        override fun read(output: ByteArray, position: Long, offset: Int, count: Int): Int = try {
            this.channel().use { it.read(output, position, offset, count) }
        } catch (e: IOException) {
            throw ExodusException("Failed to read from file $path", e)
        }

        /**
         * Returns this [FileChannelBlock].
         *
         * @return [FileChannelBlock]
         */
        override fun refresh() = this

        /**
         * Provides access to the [SharedFileChannel] underpinning this [FileChannelBlock].
         *
         * @return [SharedFileChannel]
         */
        internal fun channel(): SharedFileChannel = SharedFileChannelCache.getCachedFile(this.path)
    }
}