package org.vitrivr.cottontail.cli.entity

import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.ddl.ListEntities
import org.vitrivr.cottontail.utilities.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * List all [org.vitrivr.cottontail.dbms.entity.DefaultEntity] stored in Cottontail DB.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 2.0.0
 */
@ExperimentalTime
class ListAllEntitiesCommand(val client: SimpleClient) : AbstractCottontailCommand.System(name = "all", help = "Lists all entities stored in Cottontail DB.") {
    override fun exec() {
        /* Execute query. */
        val timedTable = measureTimedValue {
            TabulationUtilities.tabulate(this.client.list(ListEntities()))
        }

        /* Output results. */
        println("${timedTable.value.rowCount - 1} entities found (took ${timedTable.duration}).")
        print(timedTable.value)
    }
}