package org.vitrivr.cottontail.database.general

/**
 * Enum listing the different [DBOVersion]s for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class DBOVersion {
    /** Undefined [DBOVersion]. Used as placeholder in certain areas. */
    UNDEFINED,

    /** The first, legacy version of the Cottontail DB  data organisation. */
    V1_0,

    /** The second, iteration of the Cottontail DB  data organisation which was introduced in preparation of the HARE column format. */
    V2_0,

    /** The third, iteration of the Cottontail DB data organisation, which was introduces with transition from MapDB to Xodus. */
    V3_0
}