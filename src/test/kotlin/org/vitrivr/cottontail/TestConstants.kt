package org.vitrivr.cottontail

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.config.MapDBConfig
import java.nio.file.Paths

/**
 * Some constants used during execution of unit tests.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object TestConstants {

    init {
        println((System.getProperty("enableMmap") ?: "true").toBoolean())
    }

    /** Location of the Cottontail DB data folder used for testing. */
    val config = Config(root = Paths.get("./cottontaildb-test"), cli = false, mapdb = MapDBConfig(enableMmap = (System.getProperty("enableMmap") ?: "true").toBoolean()))

    /** General size of collections used for testing. */
    const val collectionSize: Int = 100_000

    /** Maximum dimension used for vector generation. */
    const val smallVectorMaxDimension: Int = 128

    /** Maximum dimension used for vector generation. */
    const val mediumVectorMaxDimension: Int = 512

    /** Maximum dimension used for vector generation. */
    const val largeVectorMaxDimension: Int = 2048
}
