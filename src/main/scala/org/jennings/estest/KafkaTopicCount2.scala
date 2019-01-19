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

object KafkaTopicCount2 {

  private val log = LogFactory.getLog(this.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      System.err.println("Usage: SendKafkaTopicCassandra <sparkMaster> <emitIntervalInMillis>" +
        " <kafkaBrokers> <kafkaConsumerGroup> <kafkaTopics> <kafkaThreads>")
      System.exit(1)
    }

    val sparkMaster = args(0)
    val emitInterval = args(1)
    val kBrokers = args(2)
    val kConsumerGroup = args(3)
    val kTopics = args(4)
    val kThreads = args(5)



    // configuration
    val sConf = new SparkConf(true)
      .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(sparkMaster, "KafkaToDSE", sConf)



    // the streaming context
    val ssc = new StreamingContext(sc, Milliseconds(emitInterval.toInt))


    // create the kafka stream
    val stream = createKafkaStream(ssc, kBrokers, kConsumerGroup, kTopics, kThreads.toInt, "latest")

    // very specific adaptation for performance
    val dataStream = stream.map(line => adaptSpecific(line))


    var total = 0L
    println("ts|count|total")

    dataStream.foreachRDD {
      (rdd, time) =>

        val count = rdd.count()
        total += count
        if (count > 0) {

          val msg = "%s|%s|%s".format(time, count, total)
          log.warn(msg)
          println(msg)
        }

    }



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
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()

    val id = uuid.toString              // NOTE: This is to ensure unique records
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

    // FIXME: why do we need to convert to tuples? why cant we store the data as a map?
    val data = (id, ts, speed, dist, bearing, rtid, orig, dest, secsToDep, latitude, longitude, geometryText)
    //println(data)
    data
  }

}
