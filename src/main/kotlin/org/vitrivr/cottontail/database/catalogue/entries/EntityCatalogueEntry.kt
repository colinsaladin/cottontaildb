package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.*
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Name
import java.io.ByteArrayInputStream

/**
 * A [EntityCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityCatalogueEntry(val name: Name.EntityName, val created: Long, val columns: List<Name.ColumnName>, val indexes: List<Name.IndexName>): Comparable<EntityCatalogueEntry> {
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<Nothing> {
            val entityName = Name.EntityName.Binding.readObject(stream)
            val created = LongBinding.BINDING.readObject(stream)
            val columns = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.ColumnName.Binding.readObject(stream)
            }
            val indexes = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.IndexName.Binding.readObject(stream)
            }
            return EntityCatalogueEntry(entityName, created, columns, indexes)
        }
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is EntityCatalogueEntry) { "$`object` cannot be written as entity entry." }
            Name.EntityName.Binding.writeObject(output, `object`.name)
            LongBinding.writeCompressed(output, `object`.created)

            /* Write all columns. */
            ShortBinding.BINDING.writeObject(output,`object`.columns.size)
            for (columnName in `object`.columns) {
                Name.ColumnName.Binding.writeObject(output, columnName)
            }

            /* Write all indexes. */
            ShortBinding.BINDING.writeObject(output,`object`.indexes.size)
            for (indexName in `object`.indexes) {
                Name.IndexName.Binding.writeObject(output, indexName)
            }
        }
    }

    override fun compareTo(other: EntityCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())
}