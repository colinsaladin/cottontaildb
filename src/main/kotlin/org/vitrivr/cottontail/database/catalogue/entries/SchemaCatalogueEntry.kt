package org.vitrivr.cottontail.database.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.model.basics.Name
import java.io.ByteArrayInputStream

/**
 * A [SchemaCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class SchemaCatalogueEntry(val name: Name.SchemaName): Comparable<SchemaCatalogueEntry> {
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream) = SchemaCatalogueEntry(Name.SchemaName(StringBinding.BINDING.readObject(stream)))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is SchemaCatalogueEntry) { "$`object` cannot be written as schema entry." }
            StringBinding.BINDING.writeObject(output, `object`.name.components[1])
        }
    }
    override fun compareTo(other: SchemaCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())
}