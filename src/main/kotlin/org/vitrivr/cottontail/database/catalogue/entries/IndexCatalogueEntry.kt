package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.index.basics.IndexState

import org.vitrivr.cottontail.database.index.basics.IndexType
import org.vitrivr.cottontail.model.basics.Name
import java.io.ByteArrayInputStream

/**
 * A [IndexCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexCatalogueEntry(val name: Name.IndexName, val type: IndexType, val state: IndexState, val columns: Array<Name.ColumnName>, val config: Map<String,String>): Comparable<IndexCatalogueEntry> {
    companion object: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): IndexCatalogueEntry {
            val entityName = Name.IndexName.Binding.readObject(stream)
            val type = IndexType.values()[IntegerBinding.readCompressed(stream)]
            val state = IndexState.values()[IntegerBinding.readCompressed(stream)]
            val columns = (0 until ShortBinding.BINDING.readObject(stream)).map {
                Name.ColumnName.Binding.readObject(stream)
            }.toTypedArray()
            val entries = (0 until ShortBinding.BINDING.readObject(stream)).associate {
                StringBinding.BINDING.readObject(stream) to StringBinding.BINDING.readObject(stream)
            }
            return IndexCatalogueEntry(entityName, type, state, columns, entries)
        }
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is IndexCatalogueEntry) { "$`object` cannot be written as index entry." }
            Name.EntityName.Binding.writeObject(output, `object`.name)
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.state.ordinal)

            /* Write all columns. */
            ShortBinding.BINDING.writeObject(output,`object`.columns.size)
            for (columnName in `object`.columns) {
                Name.ColumnName.Binding.writeObject(output, columnName)
            }

            /* Write all indexes. */
            ShortBinding.BINDING.writeObject(output,`object`.config.size)
            for (entry in `object`.config) {
                StringBinding.BINDING.writeObject(output, entry.key)
                StringBinding.BINDING.writeObject(output, entry.value)
            }
        }
    }

    override fun compareTo(other: IndexCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IndexCatalogueEntry

        if (name != other.name) return false
        if (type != other.type) return false
        if (state != other.state) return false
        if (!columns.contentEquals(other.columns)) return false
        if (config != other.config) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + state.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + config.hashCode()
        return result
    }
}