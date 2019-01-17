package org.jennings.estest

import java.io.InputStream
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.util.Random

object SendKafkaTopicTimescale {
  private val log = LogFactory.getLog(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length < 11) {
      System.err.println("Usage: SendKafkaTopicTimescale <sparkMaster> <emitIntervalInMillis>" +
          " <kafkaBrokers> <kafkaConsumerGroup> <kafkaTopics> <kafkaThreads> <timescaleHost> <recreateTable> <debug> (<latest=true> <schema=realtime> <table=planes>)")
      System.exit(1)
    }

    val sparkMaster = args(0)
    val emitInterval = args(1)
    val kBrokers = args(2)
    val kConsumerGroup = args(3)
    val kTopics = args(4)
    val kThreads = args(5)
    val kTimescaleHost = args(6)
    val recreateTable = args(7).toBoolean
    val kDebug = args(8).toBoolean
    // default latest to true
    val kLatest = if (args.length > 9) args(0).toBoolean else true
    val kSchema = if (args.length > 10) args(10) else "realtime"
    val kTable = if (args.length > 11) args(11) else "planes"

    // configuration
    val sConf = new SparkConf(true)
        .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(sparkMaster, "KafkaToTimescale", sConf)

    val schema = kSchema
    val table = kTable

    val url = s"jdbc:postgresql://$kTimescaleHost:5432/$schema"
    val properties = new Properties
    properties.put("user", "realtime")
    properties.put("password", "esri.test")

    // check if need to recreate the tables
    if (recreateTable) {
      log.info(s"We are recreating the table: $schema.$table")
      println(s"We are recreating the table: $schema.$table")
      classOf[org.postgresql.Driver]
      val connection = DriverManager.getConnection(url, properties)

      val statement = connection.createStatement()

      statement.execute(s"CREATE SCHEMA IF NOT EXISTS $schema")
      statement.execute(s"DROP TABLE IF EXISTS $schema.$table")

      // FiXME: Dynamically create the CREATE TABLE sql based on schema
      statement.execute(s"""
      CREATE TABLE IF NOT EXISTS $schema.$table
      (
        objectid    BIGSERIAL,
        globalid    UUID,
        id          TEXT,
        ts          TIMESTAMP NOT NULL,
        speed       DOUBLE PRECISION,
        dist        DOUBLE PRECISION,
        bearing     DOUBLE PRECISION,
        rtid        INTEGER,
        orig        TEXT,
        dest        TEXT,
        secstodep   INTEGER,
        lon         DOUBLE PRECISION,
        lat         DOUBLE PRECISION,
        geometry    GEOMETRY(POINT, 4326)
      )""".stripMargin
      )

      statement.execute(s"select create_hypertable('$schema.$table', 'ts')")

    }

    log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")

    // the streaming context
    val ssc = new StreamingContext(sc, Milliseconds(emitInterval.toInt))

    // resetToSt
    val resetToStr = if (kLatest) "latest" else "earliest"

    // create the kafka stream
    val stream = createKafkaStream(ssc, kBrokers, kConsumerGroup, kTopics, kThreads.toInt, resetToStr)

    //using COPY command so timestamps have to be formatted as dates
    val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    // very specific adaptation for performance
    val dataStream = stream.map(line => adaptSpecific(line, simpleFormat))

    // debug
    if (kDebug) {
      dataStream.foreachRDD {
        (rdd, time) =>
          val count = rdd.count()
          if (count > 0) {
            val msg = "Time %s: saving to Timescale (%s total records)".format(time, count)
            log.warn(msg)
            println(msg)
          }
      }
    }

    // save to timescale
    dataStream.foreachRDD {
      (rdd, _) =>
        rdd.foreachPartition( iterator => {
          classOf[org.postgresql.Driver]
          val connection = DriverManager.getConnection(url, properties)
          val copyManager = new CopyManager(connection.asInstanceOf[BaseConnection])
          val copySql = s"""COPY $schema.$table (globalid,ts,speed,dist,bearing,rtid,orig,dest,secstodep,lon,lat,geometry ) FROM STDIN WITH (NULL 'null', FORMAT CSV, DELIMITER ',')"""

          val rowsCopied = copyManager.copyIn(copySql, rddToInputStream(iterator))

          val msg = s"Inserted $rowsCopied records"
          log.warn(msg)
          println(msg)

          connection.close()
        })
    }


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

  private def rddToInputStream(rows: Iterator[Array[Byte]]) = {
    val bytes = rows.flatten
    new InputStream {
      override def read(): Int = if (bytes.hasNext) {
        bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
      } else {
        -1
      }
    }
  }
  /**
    * Adapt to the very specific Safegraph Schema
    */
  private def adaptSpecific(line: String, dateFormat: SimpleDateFormat) = {
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()
    val id = uuid.toString              // NOTE: This is to ensure unique records
    val ts = dateFormat.format(new Date(row(1).toLong))
    val speed = row(2).toDouble
    val dist = row(3).toDouble
    val bearing = row(4).toDouble
    val rtid = row(5).toInt
    val orig = row(6)
    val dest = row(7)
    val secsToDep = row(8).toInt
    val longitude = row(9).toDouble
    val latitude = row(10).toDouble

    val geometryText = "SRID=4326;POINT (" + row(9) + " " + row(10) + ")"
    s"""$id,"$ts",$speed,$dist,$bearing,$rtid,"$orig","$dest",$secsToDep,$longitude,$latitude,"$geometryText"\n""".getBytes
  }
}