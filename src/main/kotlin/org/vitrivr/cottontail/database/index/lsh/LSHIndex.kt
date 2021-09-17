package org.vitrivr.cottontail.database.index.lsh

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.index.basics.AbstractHDIndex
import org.vitrivr.cottontail.database.index.basics.AbstractIndex
import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.nio.file.Path

abstract class LSHIndex<T : VectorValue<*>>(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {
    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    final override val produces: Array<ColumnDef<*>> = emptyArray()

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.LSH
}