package org.jennings.estest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{KafkaUtils, PreferConsistent}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.elasticsearch.spark.rdd.EsSpark


/**
  * Created by david on 11/4/17.
  */
object SendKafkaTopicElasticsearch {

  // spark-submit --class org.jennings.sparktest.SendKafkaTopicElasticsearch target/sparktest.jar

  // java -cp target/sparktest.jar org.jennings.estest.SendKafkaTopicElasticsearch

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 7 && numargs != 9) {
      System.err.println("Usage: SendKafkaTopicElasticsearch broker topic ESServer ESPort SpkMaster StreamingIntervalSec (Username) (Password)")
      System.err.println("        broker: Server:IP")
      System.err.println("        topic: Kafka Topic Name")
      System.err.println("        ESServer: Elasticsearch Server Name or IP")
      System.err.println("        ESPort: Elasticsearch Port (e.g. 9200)")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        IndexType: Index/Type (e.g. planes/events")
      System.err.println("        Spark Streaming Interval Seconds (e.g. 1")
      System.err.println("        Username: Elasticsearch Username (optional)")
      System.err.println("        Password: Elasticsearch Password (optional)")
      System.exit(1)

    }

    val broker = args(0)
    val topic = args(1)
    val esServer = args(2)
    val esPort = args(3)
    val spkMaster = args(4)
    val indexAndType = args(5)
    val sparkStreamSeconds = args(6).asInstanceOf[Long]

    //val Array(filename,esServer,esPort,spkMaster,indexAndType) = args

    println("Sending " + topic + " to " + esServer + ":" + esPort + " using " + spkMaster)

    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", esServer)
    sparkConf.set("es.port", esPort)
    // Without the following it would not create the index on single-node mode
    sparkConf.set("es.nodes.discovery", "false")
    sparkConf.set("es.nodes.data.only", "false")
    // Without setting es.nodes.wan.only the index was created but loading data failed (5.5.1)
    sparkConf.set("es.nodes.wan.only", "true")


    if (numargs == 9) {
      val username = args(7)
      val password = args(8)
      sparkConf.set("es.net.http.auth.user", username)
      sparkConf.set("es.net.http.auth.pass", password)
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

    stream.foreachRDD { rdd =>

      //rdd.map(_.value())

      val tsRDD = rdd.map(_.value)
      EsSpark.saveJsonToEs(tsRDD, indexAndType)

      // Error KafkaConsumer is not thread safe errors when trying the following

//      val thread = new Thread {
//        override def run: Unit = {
//          EsSpark.saveJsonToEs(tsRDD, indexAndType)
//        }
//      }
//      thread.start
//      println(rdd.count());
//      rdd.foreach { f =>
//        println(f.value())
//      }
    }

    ssc.start()

    ssc.awaitTermination()

  }
}
