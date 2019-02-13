package org.jennings.estest

import java.util.UUID

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object SendKafkaTopicCassandraPlanesHashGlobalObjectIds {

  private val log = LogFactory.getLog(this.getClass)
  private var objectId = 0

  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println(
        "Usage: SendKafkaTopicCassandraPlanesHashGlobalObjectIds " +
            "<sparkMaster> <emitIntervalInMillis> " +
            "<kafkaBrokers> <kafkaConsumerGroup> <kafkaTopics> <kafkaThreads> <cassandraHost> " +
            "<replicationFactor> <recreateTable> <storeGeo> <debug> " +
            "<compactionInMinutes=-1> <ttlInSec=-1> <gc_grace_seconds=0> <consistencyLevel=ANY> " +
            "<latest=true> <keyspace=realtime> <table=planes>"
      )
      System.exit(1)
    }

    val sparkMaster = args(0)
    val emitInterval = args(1)
    val kBrokers = args(2)
    val kConsumerGroup = args(3)
    val kTopics = args(4)
    val kThreads = args(5)
    val kCassandraHost = args(6)
    val kReplicationFactor = args(7)
    val recreateTable = args(8).toBoolean
    val storeGeo = args(9).toBoolean
    val kDebug = args(10).toBoolean

    // default the optional argument values
    val compactionInMinutes = if (args.length > 11) args(11).toLong else -1
    val ttlInSec = if (args.length > 12) args(12).toLong else -1
    val gcGraceSeconds = if (args.length > 13) args(13) else 0
    val consistencyLevel = if (args.length > 14) args(14) else ConsistencyLevel.ANY.toString
    val kLatest = if (args.length > 15) args(15).toBoolean else true
    val kKeyspace = if (args.length > 16) args(16) else "realtime"
    val kTable = if (args.length > 17) args(17) else "planes"


    val useSolr = storeGeo
    println("Using Solr ? " + useSolr)

    // configuration
    val sConf = new SparkConf(true)
        .set("spark.cassandra.connection.host", kCassandraHost)
        .set("spark.cassandra.output.consistency.level", consistencyLevel)
        .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(sparkMaster, "KafkaToDSE", sConf)

    val keyspace = kKeyspace
    val table = kTable

    // check if need to recreate the tables
    if (recreateTable) {
      log.info(s"We are recreating the table: $keyspace.$table")
      println(s"We are recreating the table: $keyspace.$table")
      CassandraConnector(sConf).withSessionDo {
        session =>
          session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")
          session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $kReplicationFactor }")
          session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")

          // FiXME: Dynamically create the CREATE TABLE sql based on schema
          val createTableOnlyStr =
            s"""
              CREATE TABLE IF NOT EXISTS $keyspace.$table
              (
                globalid text,
                objectid bigint,
                plane_id text,
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
                geom_4326 text,
                esri_geohash_geohash_4326_12 text,
                esri_geohash_square_102100_30 text,
                esri_geohash_pointytriangle_102100_30 text,
                esri_geohash_flattriangle_102100_30 text,
                PRIMARY KEY (globalid, ts)
              )
            """.stripMargin

          val compactionStr =
            s"""
               compaction = {'compaction_window_size': '$compactionInMinutes',
                             'compaction_window_unit': 'MINUTES',
                             'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy'}
             """.stripMargin

          val ttlStr = s"""default_time_to_live = $ttlInSec"""

          val gcGraceStr = s"""gc_grace_seconds = $gcGraceSeconds"""  // DSE default is 864000

          val createTableStr = (compactionInMinutes > -1, ttlInSec > -1) match {
            case (true , true ) => s"""$createTableOnlyStr WITH $compactionStr AND $ttlStr AND $gcGraceStr"""
            case (true , false) => s"""$createTableOnlyStr WITH $compactionStr AND $gcGraceStr"""
            case (false, true ) => s"""$createTableOnlyStr WITH $ttlStr AND $gcGraceStr"""
            case _ => s"""$createTableOnlyStr WITH $gcGraceStr"""
          }

          session.execute(createTableStr)

          if (useSolr) {
            //
            // NOTE: LOOK AT THE SOFT COMMIT INTERVAL IN SOLR
            //
            // enable search on all fields (except geometry)
            session.execute(s"""
                 | CREATE SEARCH INDEX ON $keyspace.$table
                 | WITH COLUMNS
                 |  globalid,
                 |  objectid,
                 |  plane_id,
                 |  ts,
                 |  speed,
                 |  dist,
                 |  bearing,
                 |  rtid,
                 |  orig,
                 |  dest,
                 |  secstodep,
                 |  lon,
                 |  lat,
                 |  esri_geohash_geohash_4326_12,
                 |  esri_geohash_square_102100_30,
                 |  esri_geohash_pointytriangle_102100_30,
                 |  esri_geohash_flattriangle_102100_30
              """.stripMargin
            )

            // check if we want to store the Geo
            if (storeGeo) {
              // enable search on geometry field
              session.execute(s"""
                   |ALTER SEARCH INDEX SCHEMA ON $keyspace.$table
                   |ADD types.fieldType[ @name='rpt',
                   |                     @class='solr.SpatialRecursivePrefixTreeFieldType',
                   |                     @geo='false',
                   |                     @worldBounds='ENVELOPE(-1000, 1000, 1000, -1000)',
                   |                     @maxDistErr='0.001',
                   |                     @distanceUnits='degrees' ]
                """.stripMargin
              )
              session.execute(s"""
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

    // the streaming context
    val ssc = new StreamingContext(sc, Milliseconds(emitInterval.toInt))

    // resetToSt
    val resetToStr = if (kLatest) "latest" else "earliest"

    // create the kafka stream
    val stream = createKafkaStream(ssc, kBrokers, kConsumerGroup, kTopics, kThreads.toInt, resetToStr)

    // very specific adaptation for performance
    val dataStream = stream.map(line => adaptSpecific(line))

    // debug
    if (kDebug) {
      dataStream.foreachRDD {
        (rdd, time) =>
          val count = rdd.count()
          if (count > 0) {
            val msg = "Time %s: saving to DSE (%s total records)".format(time, count)
            log.warn(msg)
            println(msg)
          }
      }
    }

    // save to cassandra
    dataStream.foreachRDD {
      (rdd, _) =>
        try {
          rdd.saveToCassandra(
            keyspace,
            table,
            // FIXME: Do we need to specify all the columns?
            SomeColumns(
              "globalid",
              "objectid",
              "plane_id",
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
              "geom_4326",
              "esri_geohash_geohash_4326_12",
              "esri_geohash_square_102100_30",
              "esri_geohash_pointytriangle_102100_30",
              "esri_geohash_flattriangle_102100_30"
            )
          )
        } catch {
          case error: Throwable =>
            println("***** rdd.saveToCassandra caught the following exception: *****")
            error.printStackTrace()
            println("***** dropped the current rdd.saveToCassandra batch and continue with the next batch... *****")
        }
    }

    println(s"Running Spark Streaming Context with conf: ${sc.getConf.getAll}")
    log.info("Stream is starting now...")
    println("Stream is starting now...")

    // start the stream
    ssc.start
    ssc.awaitTermination()
  }

  // create the kafka stream
  private def createKafkaStream(ssc: StreamingContext, brokers: String, consumerGroup: String, topics: String, numOfThreads: Int = 1, resetToStr: String): DStream[String] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroup,
      "auto.offset.reset" -> resetToStr,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicMap = topics.split(",")
    val kafkaStreams = (1 to numOfThreads).map { i =>
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicMap, kafkaParams)).map(_.value())
    }
    val unifiedStream = ssc.union(kafkaStreams)
    unifiedStream
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
    objectId = objectId + 1
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()

    val globalid = uuid.toString // NOTE: This is to ensure unique records
    val plane_id = row(0)
    val ts = row(1).toLong
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
    val geohash = row(11)
    val sqrhash = row(12)
    val pntytrihash = row(13)
    val flattrihash = row(14)

    // FIXME: why do we need to convert to tuples? why cant we store the data as a map?
    val data = (globalid, objectId, plane_id, ts, speed, dist, bearing, rtid, orig, dest, secsToDep, latitude, longitude, geometryText, geohash, sqrhash, pntytrihash, flattrihash)
    //println(data)
    data
  }

}
