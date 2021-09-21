package org.vitrivr.cottontail.storage.io

import jetbrains.exodus.ExodusException
import jetbrains.exodus.core.dataStructures.Pair
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.EnvironmentImpl
import jetbrains.exodus.io.DataReader
import jetbrains.exodus.io.DataReaderWriterProvider
import jetbrains.exodus.io.DataWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * A [DataReaderWriterProvider] implementation for our custom [FileChannelDataReader] and [FileChannelDataWriter].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FileChannelDataReaderWriterProvider : DataReaderWriterProvider() {



    companion object {

        @JvmStatic
        protected fun checkDirectory(location: String): Path {
            val directory = Paths.get(location)
            if (Files.isRegularFile(directory)) {
                throw ExodusException("A directory is required: $directory")
            }
            if (!Files.exists(directory)) {
                try {
                    Files.createDirectories(directory)
                } catch (e: Throwable) {
                    throw ExodusException("Failed to create directory $directory due to error: ${e.message}")
                }
            }
            return directory
        }
    }

    /** The [EnvironmentImpl] used by this [FileChannelDataReaderWriterProvider]. Initialized lazily. */
    private var env: EnvironmentImpl? = null

    /**
     * Called once a new [Environment] is available.
     *
     * @param environment The [Environment] that is using this [FileChannelDataReaderWriterProvider].
     */
    override fun onEnvironmentCreated(environment: Environment) {
        super.onEnvironmentCreated(environment)
        this.env = environment as EnvironmentImpl
    }

    /**
     * Creates and returns a new [Pair] of [FileChannelDataReader] and [FileChannelDataWriter].
     *
     * @param directory The path to the directory location.
     */
    override fun newReaderWriter(directory: String): Pair<DataReader, DataWriter> {
        val reader = newReader(checkDirectory(directory))
        return Pair(reader, newWriter(reader as FileChannelDataReader))
    }

    /**
     * Creates and returns a new  [FileChannelDataReader]
     *
     * @param directory The [Path] to the directory location.
     */
    private fun newReader(directory: Path): DataReader {
        val reader = FileChannelDataReader(directory)
        reader.log = this.env?.log
        return reader
    }

    /**
     * Creates and returns a new  [FileChannelDataWriter]
     *
     * @param reader The associated [FileChannelDataReader] instance.
     */
    private fun newWriter(reader: FileChannelDataReader): DataWriter = FileChannelDataWriter(reader)
}