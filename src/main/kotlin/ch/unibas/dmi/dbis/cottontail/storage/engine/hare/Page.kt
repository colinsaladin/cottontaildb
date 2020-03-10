package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.EMPTY
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page.Constants.PAGE_DATA_SIZE_BYTES

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.optimisticRead
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import it.unimi.dsi.fastutil.longs.Long2BooleanMaps
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet
import it.unimi.dsi.fastutil.longs.LongArraySet
import it.unimi.dsi.fastutil.longs.LongSets
import org.apache.lucene.util.LongBitSet

import java.nio.ByteBuffer
import java.util.concurrent.locks.StampedLock

import kotlin.math.max

/**
 * This is a wrapper for an individual data [Page] managed by the HARE storage engine. At their core,
 * [Page]s are mere chunks of data with a fixed size of 4096 bytes each. However, each [Page] has a
 * 16 byte header used for flags and properties set by the storage engine and used internally.
 *
 * [Page]s are backed by a [ByteBuffer] object. That [ByteBuffer] must have a capacity of exactly
 * 4096 bytes, otherwise an exception will be thrown. The [ByteBuffer] backing a page is rewinded
 * when handed to the [Page]'s constructor. It is not recommended to use that [ByteBuffer] outside
 * of the [Page]s context.
 *
 * @see DiskManager
 * @see BufferPool
 *
 * @version 1.0
 * @author Ralph Gasser
 */
class Page(val data: ByteBuffer) {

    /** Some constants related to [Page]s. */
    object Constants {
        /** The number of bits to shift in order to get the page size (i.e. the N in 2^N) . */
        const val PAGE_BIT_SHIFT = 12

        /** The size of a [Page]. This value is constant.*/
        const val PAGE_DATA_SIZE_BYTES = 1 shl PAGE_BIT_SHIFT

        /** The size of a [Page]. This value is constant.*/
        val EMPTY = ByteArray(PAGE_DATA_SIZE_BYTES)
    }

    init {
        /** Rewind ByteBuffer. */
        this.data.rewind()
        require(this.data.capacity() == PAGE_DATA_SIZE_BYTES) { throw IllegalArgumentException("A Page object must be backed by a ByteBuffer of exactly 4096 bytes.")}
    }

    /** The identifier for the [Page]. A value of -1 means, that this page is empty (i.e. uninitialized). */
    @Volatile
    var id: PageId = System.currentTimeMillis()
        internal set

    /** Internal flag: Indicating whether this [Page] has uncommitted changes. */
    @Volatile
    var dirty: Boolean = false
        internal set

    fun getBytes(index: Int, bytes: ByteArray) : ByteArray {
        this.data.position(index).get(bytes).rewind()
        return bytes
    }

    fun getBytes(index: Int, limit: Int) : ByteArray = getBytes(index, ByteArray(max(PAGE_DATA_SIZE_BYTES, limit-index)))
    fun getBytes(index: Int) : ByteArray = getBytes(index, PAGE_DATA_SIZE_BYTES)
    fun getByte(index: Int): Byte = this.data.get(index)
    fun getShort(index: Int): Short = this.data.getShort(index)
    fun getChar(index: Int): Char = this.data.getChar(index)
    fun getInt(index: Int): Int = this.data.getInt(index)
    fun getLong(index: Int): Long = this.data.getLong(index)
    fun getFloat(index: Int): Float = this.data.getFloat(index)
    fun getDouble(index: Int): Double =  this.data.getDouble(index)

    /**
     * Writes a [Byte] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Byte] value to write.
     * @return This [Page]
     */
    fun putByte(index: Int, value: Byte): Page {
        this.data.put(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [ByteArray] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [ByteArray] value to write.
     * @return This [Page]
     */
    fun putBytes(index: Int, value: ByteArray): Page {
        this.data.mark().position(index).put(value).rewind()
        this.dirty = true
        return this
    }

    /**
     * Writes a [Short] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Short] value to write.
     * @return This [Page]
     */
    fun putShort(index: Int, value: Short): Page {
        this.data.putShort(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [Char] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Char] value to write.
     * @return This [Page]
     */
    fun putChar(index: Int, value: Char): Page {
        this.data.putChar(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [Int] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Int] value to write.
     * @return This [Page]
     */
    fun putInt(index: Int, value: Int): Page {
        this.data.putInt(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [Long] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Long] value to write.
     * @return This [Page]
     */
    fun putLong(index: Int, value: Long): Page {
        this.data.putLong(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [Float] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Float] value to write.
     * @return This [Page]
     */
    fun putFloat(index: Int, value: Float): Page {
        this.data.putFloat(index, value)
        this.dirty = true
        return this
    }

    /**
     * Writes a [Double] to the given position and sets the dirty flag to true.
     *
     * @param index Position to write byte to.
     * @param value New [Double] value to write.
     * @return This [Page]
     */
    fun putDouble(index: Int, value: Double): Page {
        this.data.putDouble(index, value)
        this.dirty = true
        return this
    }

    /**
     *
     */
    fun clear() {
        this.data.position(0).put(EMPTY).rewind()
        this.dirty = true
    }
}


