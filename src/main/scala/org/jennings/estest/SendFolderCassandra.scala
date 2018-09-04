package org.jennings.estest

import java.io.File
import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}


import scala.util.Random

object SendFolderCassandra {


  private val log = LogFactory.getLog(this.getClass)


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 7) {
      System.err.println("Usage: SendFolderCassandra foldername [cassandraHost] [replicationFactor] [recreateTable] [useSolr] [storeGeo] [Spark Master] ")
      System.err.println("        foldername: File Pattern")
      System.err.println("        cassandraHost: Cassandra Server Name or IP")
      System.err.println("        replicationFactor: Cassandra Replication Factor")
      System.err.println("        recreateTable: Delete and create table")
      System.err.println("        useSolr: Add Search Index")
      System.err.println("        storeGeo: Convert Lat/Lon into Geometry")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")

      System.exit(1)

    }

    val foldername = args(0)
    val cassandraHost = args(1)
    val replicationFactor = args(2)
    val recreateTable = args(3)
    val useSolr = args(4)
    val storeGeo = args(5)
    val spkMaster = args(6)



    // configuration
    val sConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.output.consistency.level", ConsistencyLevel.ONE.toString)
      .setAppName(getClass.getSimpleName)

    val keyspace = "realtime"
    val table = "planes"

    // check if need to recreate the tables
    if (recreateTable.toBoolean) {
      log.info(s"We are recreating the table: $keyspace.$table")
      println(s"We are recreating the table: $keyspace.$table")
      CassandraConnector(sConf).withSessionDo {
        session =>
          session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")
          session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $replicationFactor }")
          session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")

          // FiXME: Dynamically create the CREATE TABLE sql based on schema
          session.execute(s"""
          CREATE TABLE IF NOT EXISTS $keyspace.$table
          (
            id bigint,
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
            PRIMARY KEY ((id), ts)
          ) WITH CLUSTERING ORDER BY (ts DESC)"""
          )

          if (useSolr.toBoolean) {
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

            // check if we want to store the Geo
            if (storeGeo.toBoolean) {
              // enable search on geometry field
              session.execute(
                s"""
                   |ALTER SEARCH INDEX SCHEMA ON $keyspace.$table
                   |ADD types.fieldType[ @name='rpt',
                   |                     @class='solr.SpatialRecursivePrefixTreeFieldType',
                   |                     @geo='false',
                   |                     @worldBounds='ENVELOPE(-1000, 1000, 1000, -1000)',
                   |                     @maxDistErr='0.001',
                   |                     @distanceUnits='degrees' ]
             """.stripMargin
              )
              session.execute(
                s"""
                   |ALTER SEARCH INDEX SCHEMA ON $keyspace.$table
                   |ADD fields.field[ @name='geometry',
                   |                  @type='rpt',
                   |                  @indexed='true',
                   |                  @stored='true' ];
             """.stripMargin
              )
              session.execute(
                s"RELOAD SEARCH INDEX ON $keyspace.$table"
              )
            }
          }
      }
    }

    log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")



    val sc = new SparkContext(spkMaster, "SendFileCassandra", sConf)

    val folder = new File(foldername)

    val fileArray = folder.list()

    if (fileArray == null) {
      println("No files in %s".format(folder))
    } else {
      val files = fileArray.sortWith(_ < _).iterator

      while (files.hasNext) {
        // For each file in the folder
        val file = files.next
        println(file)
        val filename = foldername + File.separator + file
        val textFile =  sc.textFile(filename).map(line => adaptSpecific(line))

        textFile.saveToCassandra(
          keyspace,
          table,
          SomeColumns(
            "id",
            "ts",
            "speed",
            "dist",
            "bearing",
            "rtid",
            "orig",
            "dest",
            "secstodep",
            "lon",
            "lat",
            "geometry"
          )
        )

      }
    }

  }

  // used to generate a random uuid
  private val RANDOM = new Random()

  // initialized the object mapper / text parser only once
  private val objectMapper = {
    // create an empty schema
    val schema = CsvSchema.emptySchema()
      .withColumnSeparator(',')
      .withLineSeparator("\\n")
    // create the mapper
    val csvMapper = new CsvMapper()
    csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
    csvMapper
      .readerFor(classOf[Array[String]])
      .`with`(schema)
  }

  /**
    * Adapt to the very specific Safegraph Schema
    */
  private def adaptSpecific(line: String) = {

    //println(line)
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()

    val id = row(0).toLong              // Use planes00001 with 1 million unique id's 0 to 999,999.
    val ts = System.currentTimeMillis();  // Current time im ms from epoch; With 1,000,000 unique id's the combination of id/ts will unique even for rates of several 100 million per second
    val speed = row(2).toDouble
    val dist = row(3).toDouble
    val bearing = row(4).toDouble
    val rtid = row(5).toInt
    val orig = row(6)
    val dest = row(7)
    val secsToDep = row(8).toInt
    val longitude = row(9).toDouble
    val latitude = row(10).toDouble
    val geometryText = "POINT (" + row(9) + " " + row(10) + ")"

    // FIXME: why do we need to convert to tuples? why cant we store the data as a map?
    val data = (id, ts, speed, dist, bearing, rtid, orig, dest, secsToDep, latitude, longitude, geometryText)
    //println(data)
    data
  }


}
