package org.vitrivr.cottontail.legacy.v2.column

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header data structure of any [ColumnV2]
 *
 * @see ColumnV2
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class ColumnV2Header(
    val columnDef: ColumnDef<*>,
    val count: Long = 0L,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis()
) {
    companion object Serializer : org.mapdb.Serializer<ColumnV2Header> {
        override fun serialize(out: DataOutput2, value: ColumnV2Header) {
            out.packInt(DBOVersion.V2_0.ordinal)
            ColumnDef.serialize(out, value.columnDef)
            out.writeLong(value.count)
            out.writeLong(value.created)
            out.writeLong(value.modified)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnV2Header {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            val def = ColumnDef.deserialize(input, available)
            return ColumnV2Header(def, input.readLong(), input.readLong(), input.readLong())
        }
    }
}