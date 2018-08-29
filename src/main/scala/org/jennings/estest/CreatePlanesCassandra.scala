package org.jennings.estest


import com.datastax.driver.core.Cluster
import org.apache.commons.logging.LogFactory

/*

  This object creates keyspace "realtime" and table "planes".

 */

object CreatePlanesCassandra {

  private val log = LogFactory.getLog(this.getClass)


  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.err.println("Usage: CreatePlanesCassandra <cassandraHost> <replicationFactor> <useSolr>")
      System.exit(1)
    }

    val Array(cassandraHost, replicationFactor, useSolrInput) = args
    val useSolr = useSolrInput.toBoolean

    val keyspace = "realtime"
    val table = "planes"

    log.info(s"We are recreating the table: $keyspace.$table")
    println(s"We are recreating the table: $keyspace.$table")


    val cluster = Cluster.builder().addContactPoint(cassandraHost).build()

    val session = cluster.connect()

    // Drop keyspace if exists
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")

    // Create Keyspace
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $replicationFactor }")

    // Drop table if exists
    session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")

    println("Before Create Table")

    session.execute(
      s"""
          CREATE TABLE IF NOT EXISTS $keyspace.$table
          (
            id text,
            ts timestamp,
            speed double,
            dist double,
            bearing double,
            rtid int,
            orig text,
            dest text,
            secstodep int,
            lon double,
            lat double,
            geometry text,

            PRIMARY KEY (id, ts)
          )"""
    )

    println("Before useSolr")

    if (useSolr) {
      // enable search on all fields (except geometry)
      session.execute(
        s"""
           | CREATE SEARCH INDEX ON $keyspace.$table
           | WITH COLUMNS
           |  id,
           |  ts,
           |  speed,
           |  dist,
           |  bearing,
           |  rtid,
           |  orig,
           |  dest,
           |  secstodep,
           |  lon,
           |  lat
           """.stripMargin
      )
    }

    println("The End")

    System.exit(0)

  }

}
