package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.statistics.columns.*
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import java.io.ByteArrayInputStream

/**
 * A [StatisticsCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class StatisticsCatalogueEntry(val name: Name.ColumnName, val type: Type<*>, val statistics: ValueStatistics<*>): Comparable<StatisticsCatalogueEntry>  {
    /**
     * Creates a [StatisticsCatalogueEntry] from the provided [ColumnDef].
     *
     * @param [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, when(def.type){
        Type.Boolean -> BooleanValueStatistics()
        Type.Byte -> ByteValueStatistics()
        Type.Short -> ShortValueStatistics()
        Type.Date -> DateValueStatistics()
        Type.Double -> DoubleValueStatistics()
        Type.Float -> FloatValueStatistics()
        Type.Int -> IntValueStatistics()
        Type.Long -> LongValueStatistics()
        Type.String -> StringValueStatistics()
        is Type.BooleanVector -> BooleanVectorValueStatistics(def.type)
        is Type.DoubleVector -> DoubleVectorValueStatistics(def.type)
        is Type.FloatVector -> FloatVectorValueStatistics(def.type)
        is Type.IntVector -> IntVectorValueStatistics(def.type)
        is Type.LongVector -> LongVectorValueStatistics(def.type)
        else -> ValueStatistics(def.type)
    })

    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream):StatisticsCatalogueEntry {
            val name = Name.ColumnName.readObject(stream)
            val type = Type.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream))
            val statistics = ValueStatistics.read(stream, type)
            return StatisticsCatalogueEntry(name, type, statistics)
        }
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is StatisticsCatalogueEntry) { "$`object` cannot be written as statistics entry." }
            Name.ColumnName.Binding.writeObject(output, `object`.name)
            IntegerBinding.writeCompressed(output, `object`.type.ordinal)
            IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
            ValueStatistics.write(output, `object`.statistics)
        }
    }

    override fun compareTo(other: StatisticsCatalogueEntry): Int = this.name.compareTo(other.name)
}