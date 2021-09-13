package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.*
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import java.io.ByteArrayInputStream

/**
 * A [ColumnCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class ColumnCatalogueEntry(val name: Name.ColumnName, val type: Type<*>, val nullable: Boolean, val primary: Boolean): Comparable<ColumnCatalogueEntry> {
    /**
     * Creates a [ColumnCatalogueEntry] from the provided [ColumnDef].
     *
     * @param [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, def.primary, def.nullable)

    /**
     * Converts this [ColumnCatalogueEntry] to a [ColumnDef].
     *
     * @return [ColumnDef] for this [ColumnCatalogueEntry]
     */
    fun toColumnDef(): ColumnDef<*> = ColumnDef(this.name, this.type, this.nullable, this.primary)

    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): ColumnCatalogueEntry = ColumnCatalogueEntry(
            Name.ColumnName.readObject(stream),
            Type.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream)),
            BooleanBinding.BINDING.readObject(stream),
            BooleanBinding.BINDING.readObject(stream),
        )
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is ColumnCatalogueEntry) { "$`object` cannot be written as column entry." }
            Name.ColumnName.Binding.writeObject(output, `object`.name)
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
            BooleanBinding.BINDING.writeObject(output, `object`.nullable)
            BooleanBinding.BINDING.writeObject(output, `object`.primary)
        }
    }

    override fun compareTo(other: ColumnCatalogueEntry): Int = this.name.compareTo(other.name)
}