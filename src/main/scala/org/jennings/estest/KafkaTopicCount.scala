package org.jennings.estest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark


/**
  * Created by david on 10/29/18.
  */
object KafkaTopicCount {

  // spark-submit --class org.jennings.sparktest.SendKafkaTopicElasticsearch target/sparktest.jar

  // java -cp target/sparktest.jar org.jennings.estest.SendKafkaTopicElasticsearch

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 4) {
      System.err.println("Usage: KafkaTopicCount broker topic ")
      System.err.println("        broker: Server:IP")
      System.err.println("        topic: Kafka Topic Name")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        Spark Streaming Interval Seconds (e.g. 1")

      System.exit(1)

    }

    val broker = args(0)
    val topic = args(1)
    val spkMaster = args(2)
    val sparkStreamSeconds = args(3).toLong

    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> broker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> java.util.UUID.randomUUID().toString(),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array(topic)

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(sparkStreamSeconds))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    var cnt = 0L

    stream.foreachRDD { rdd =>

      val tsRDD = rdd.map(_.value)
      cnt += tsRDD.count()

      println(cnt)
    }

    ssc.start()

    ssc.awaitTermination()

  }
}
