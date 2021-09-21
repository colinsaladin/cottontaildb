package org.vitrivr.cottontail.storage.io

import jetbrains.exodus.core.dataStructures.ObjectCache
import java.nio.file.Files
import java.nio.file.Path
import java.util.*

/**
 * A data structure for shared and cached access to open [SharedFileChannel] objects.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object SharedFileChannelCache  {

    /** The internal [ObjectCache] instance. */
    private lateinit var cache: ObjectCache<Path, SharedFileChannel>

    /**
     * Initializes this [SharedFileChannelCache].
     *
     * @param cacheSize Size of the [SharedFileChannelCache].
     */
    fun init(cacheSize: Int) {
        require(cacheSize > 0) { "Cache size for SharedFileChannelCache must be a positive integer value!" }
        this.cache = ObjectCache(cacheSize)
    }

    /**
     * Retrieves a [SharedFileChannel] from this [SharedFileChannelCache] and returns it. If needed, the [SharedFileChannel] is opened.
     *
     * @param path [Path] The [Path] of the [SharedFileChannel] to open.
     * @return [SharedFileChannel]
     */
    fun getCachedFile(path: Path): SharedFileChannel {
        var result: SharedFileChannel? = this.cache.newCriticalSection().use { _ ->
            val r = this.cache.tryKey(path)
            if (r != null && r.retain() > 1) {
                r.close()
                null
            } else {
                r
            }
        }

        if (result == null) {
            result = SharedFileChannel(path)
            var obsolete: SharedFileChannel? = null
            this.cache.newCriticalSection().use { _ ->
                if (this.cache.getObject(path) == null) {
                    result.retain()
                    obsolete = this.cache.cacheObject(path, result)
                }
            }
            if (obsolete != null) {
                obsolete!!.close()
            }
        }
        return result
    }

    /**
     * Removes the [SharedFileChannel] for the given [Path] from this [SharedFileChannelCache].
     *
     * @param path [Path] The [Path] for the [SharedFileChannel] to remove.
     */
    fun removeFile(path: Path) {
        val result: SharedFileChannel? = this.cache.newCriticalSection().use { _ -> this.cache.remove(path) }
        result?.close()
    }

    /**
     * Removes all [SharedFileChannel]s associated with the given directory [Path]
     *
     * @param dir The [Path] to the directory for which [SharedFileChannel]s should be removed.
     */
    fun removeDirectory(dir: Path) {
        require(Files.isDirectory(dir)) { "Failed to remove open file channels: $dir is not a directory."}
        val obsoleteFiles: MutableList<Path> = LinkedList()
        this.cache.newCriticalSection().use { _ ->
            val keys = this.cache.keys()
            while (keys.hasNext()) {
                val path = keys.next()
                if (path.parent == dir) {
                    obsoleteFiles.add(path)
                }
            }
            for (path in obsoleteFiles) {
                val obj = this.cache.getObject(path)
                this.cache.remove(path)
                obj.close()
            }
        }
    }
}