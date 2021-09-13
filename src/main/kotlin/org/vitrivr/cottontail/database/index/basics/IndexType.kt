package org.vitrivr.cottontail.database.index.basics

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.index.gg.GGIndex
import org.vitrivr.cottontail.database.index.hash.NonUniqueHashIndex
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.index.lucene.LuceneIndex
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A final list of types of [AbstractIndex] implementation.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
enum class IndexType(val inexact: Boolean) {

    BTREE_UQ(false), /* A hash based index with unique values. */

    BTREE(false), /* A hash based index. */

    LUCENE(false), /* A Lucene based index (fulltext search). */

    VAF(false), /* A VA file based index (for exact kNN lookup). */

    PQ(true), /* A product quantization based index (for approximate kNN lookup). */

    SH(true), /* A spectral hashing based index (for approximate kNN lookup). */

    LSH(true), /* A locality sensitive hashing based index for approximate kNN lookup with Lp distance. */

    LSH_SB(true), /* A super bit locality sensitive hashing based index for approximate kNN lookup with cosine distance. */

    GG(true);

    /**
     * Opens an index of this [IndexType] using the given name and [DefaultEntity].
     *
     * @param name [Name.IndexName] of the [AbstractIndex]
     * @param entity The [DefaultEntity] the desired [AbstractIndex] belongs to.
     */
    fun open(name: Name.IndexName, entity: DefaultEntity): AbstractIndex = when (this) {
        BTREE_UQ -> UniqueHashIndex(name, entity)
        BTREE -> NonUniqueHashIndex(name, entity)
        LUCENE -> LuceneIndex(name, entity)
        LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(name, entity)
        VAF -> VAFIndex(name, entity)
        PQ -> PQIndex(name, entity)
        GG -> GGIndex(name, entity)
        else -> throw NotImplementedError("Index of type $this is not implemented.")
    }

    /**
     *
     */
    fun storeConfig() = when(this) {
        BTREE -> StoreConfig.WITH_DUPLICATES_WITH_PREFIXING
        BTREE_UQ -> StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING
        LUCENE -> StoreConfig.WITHOUT_DUPLICATES
        VAF, PQ, SH, LSH, LSH_SB, GG -> StoreConfig.WITHOUT_DUPLICATES
    }
}