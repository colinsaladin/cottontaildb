package org.vitrivr.cottontail.model.basics

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed class Name : Comparable<Name> {

    companion object {
        /* Delimiter between name components. */
        const val NAME_COMPONENT_DELIMITER = "."

        /* Character used for wildcards. */
        const val NAME_COMPONENT_WILDCARD = "*"

        /* Root name component in Cottontail DB names. */
        const val NAME_COMPONENT_ROOT = "warren"
    }

    /** Internal array of [Name] components. */
    abstract val components: Array<String>

    /** Returns the last component of this [Name], i.e. the simple name. */
    open val simple: String
        get() = this.components.last()

    /** Returns the [RootName] reference. */
    val root: RootName
        get() = RootName

    /** Returns true if this [Name] matches the other [Name]. */
    abstract fun matches(other: Name): Boolean

    /**
     * The [RootName] which always is 'warren'.
     */
    object RootName : Name() {
        override val components: Array<String> = arrayOf(NAME_COMPONENT_ROOT)
        override fun matches(other: Name): Boolean = (other == this)
        fun schema(name: String) = SchemaName(NAME_COMPONENT_ROOT, name)
    }

    /**
     * A [Name] object used to identify a [Function][org.vitrivr.cottontail.functions.basics.Function].
     */
    class FunctionName(vararg components: String): Name() {
        /** Normalized [Name] components of this [SchemaName]. */
        override val components = when {
            components.size == 1 -> arrayOf(NAME_COMPONENT_ROOT, components[0].lowercase())
            components.size == 2 && components[0].lowercase() == NAME_COMPONENT_ROOT -> arrayOf(NAME_COMPONENT_ROOT, components[1].lowercase())
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid function name.")
        }

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Schema][org.vitrivr.cottontail.database.schema.DefaultSchema].
     */
    class SchemaName(vararg components: String) : Name() {

        companion object Binding: ComparableBinding() {
            override fun readObject(stream: ByteArrayInputStream) = SchemaName(StringBinding.BINDING.readObject(stream))
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                check(`object` is SchemaName) { "Cannot serialize $`object` as schema name." }
                StringBinding.BINDING.writeObject(output, `object`.components[1])
            }
        }

        /** Normalized [Name] components of this [SchemaName]. */
        override val components = when {
            components.size == 1 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].lowercase()
            )
            components.size == 2 && components[0].lowercase() == NAME_COMPONENT_ROOT -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[1].lowercase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid schema name.")
        }

        /**
         * Generates an [EntityName] as child of this [SchemaName].
         *
         * @param name Name of the [EntityName]
         * @return [EntityName]
         */
        fun entity(name: String) = EntityName(*this.components, name)

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Entity][org.vitrivr.cottontail.database.entity.DefaultEntity].
     */
    class EntityName(vararg components: String) : Name() {

        companion object Binding: ComparableBinding() {
            override fun readObject(stream: ByteArrayInputStream) = EntityName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                check(`object` is EntityName) { "Cannot serialize $`object` as schema name." }
                StringBinding.BINDING.writeObject(output, `object`.components[1])
                StringBinding.BINDING.writeObject(output, `object`.components[2])
            }
        }

        /** Normalized [Name] components of this [EntityName]. */
        override val components = when {
            components.size == 2 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].lowercase(),
                components[1].lowercase()
            )
            components.size == 3 && components[0].lowercase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].lowercase(),
                components[1].lowercase(),
                components[2].lowercase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid entity name.")
        }

        /**
         * Returns parent [SchemaName] for this [EntityName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(*this.components.copyOfRange(0, 2))

        /**
         * Generates an [IndexName] as child of this [EntityName].
         *
         * @param name Name of the [IndexName]
         * @return [IndexName]
         */
        fun index(name: String) = IndexName(*this.components, name)

        /**
         * Generates an [ColumnName] as child of this [EntityName].
         *
         * @param name Name of the [ColumnName]
         * @return [ColumnName]
         */
        fun column(name: String) = ColumnName(*this.components, name)

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [EntityName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.index.AbstractIndex].
     */
    class IndexName(vararg components: String) : Name() {

        companion object Binding: ComparableBinding() {
            override fun readObject(stream: ByteArrayInputStream) = IndexName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                check(`object` is IndexName) { "Cannot serialize $`object` as schema name." }
                StringBinding.BINDING.writeObject(output, `object`.components[1])
                StringBinding.BINDING.writeObject(output, `object`.components[2])
                StringBinding.BINDING.writeObject(output, `object`.components[3])
            }
        }

        /** Normalized [Name] components of this [IndexName]. */
        override val components = when {
            components.size == 3 -> arrayOf(NAME_COMPONENT_ROOT,
                components[0].lowercase(),
                components[1].lowercase(),
                components[2].lowercase()
            )
            components.size == 4 && components[0].lowercase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].lowercase(),
                components[1].lowercase(),
                components[2].lowercase(),
                components[3].lowercase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid index name.")
        }

        /**
         * Returns parent [SchemaName] of this [IndexName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(*this.components.copyOfRange(0, 2))

        /**
         * Returns parent  [EntityName] of this [IndexName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(*this.components.copyOfRange(0, 3))

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [IndexName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.column.Column].
     */
    class ColumnName(vararg components: String) : Name() {

        companion object Binding: ComparableBinding() {
            override fun readObject(stream: ByteArrayInputStream) = ColumnName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                check(`object` is ColumnName) { "Cannot serialize $`object` as schema name." }
                StringBinding.BINDING.writeObject(output, `object`.components[1])
                StringBinding.BINDING.writeObject(output, `object`.components[2])
                StringBinding.BINDING.writeObject(output, `object`.components[3])
            }
        }

        /** Normalized [Name] components of this [IndexName]. */
        override val components: Array<String> = when {
            components.size == 1 -> arrayOf(
                NAME_COMPONENT_ROOT,
                "*",
                "*",
                components[0].lowercase()
            )
            components.size == 2 -> arrayOf(
                NAME_COMPONENT_ROOT,
                "*",
                components[0].lowercase(),
                components[1].lowercase()
            )
            components.size == 3 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].lowercase(),
                components[1].lowercase(),
                components[2].lowercase()
            )
            components.size == 4 && components[0].lowercase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].lowercase(),
                components[1].lowercase(),
                components[2].lowercase(),
                components[3].lowercase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid column name.")
        }

        /** True if this [ColumnName] is a wildcard. */
        val wildcard: Boolean = this.components.any { it == NAME_COMPONENT_WILDCARD }

        /**
         * Returns parent [SchemaName] of this [ColumnName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName? = if (this.components[1] != NAME_COMPONENT_WILDCARD) {
            SchemaName(NAME_COMPONENT_ROOT, this.components[1])
        } else {
            null
        }

        /**
         * Returns parent [EntityName] of this [ColumnName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName? = if (this.components[2] != NAME_COMPONENT_WILDCARD) {
            EntityName(NAME_COMPONENT_ROOT, this.components[1], this.components[2])
        } else {
            null
        }

        /**
         * Checks for a match with another name. Wildcard matches  are possible for [ColumnName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean {
            if (other !is ColumnName) return false
            if (!this.wildcard) return (this == other)
            for ((i, c) in this.components.withIndex()) {
                if (c != NAME_COMPONENT_WILDCARD && c != other.components[i]) {
                    return false
                }
            }
            return true
        }

        /**
         * Transforms this [Name] to a [String]
         *
         * @return [String] representation of this [Name].
         */
        override fun toString(): String =
            if (this.components[1] == NAME_COMPONENT_WILDCARD && this.components[2] == NAME_COMPONENT_WILDCARD) {
                components[3]
            } else {
                super.toString()
            }
    }

    /**
     * Compares this [Name] with any other [Any] and returns true, if the two are equal and false otherwise.
     *
     * @param other The [Any] to compare to.
     * @return True on equality, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is Name) return false
        if (other.javaClass != this.javaClass) return false
        if (!other.components.contentEquals(this.components)) return false
        return true
    }

    /**
     * Custom hash codes for [Name] objects.
     *
     * @return Hash code for this [Name] object
     */
    override fun hashCode(): Int {
        return 42 + this.components.contentHashCode()
    }

    /**
     * Compares this [Name] to the other [Name]. Returns zero if this object is equal to
     * the specified other object, a negative number if it's less than other, or a positive
     * number if it's greater than other.
     *
     * @return Hash code for this [Name] object
     */
    override fun compareTo(other: Name): Int {
        for ((i, c) in this.components.withIndex()) {
            if (other.components.size > i) {
                val comp = c.compareTo(other.components[i])
                if (comp != 0) return comp
            } else {
                return 1
            }
        }
        return 0
    }

    /**
     * Transforms this [Name] to a [String]
     *
     * @return [String] representation of this [Name].
     */
    override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
}