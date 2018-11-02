package org.jennings.estest

import java.util.{Properties, UUID}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.jennings.estest.SendFileKafka.getClass


/**
  * Created by david on 10/29/18.
  */
object KafkaToKafka {


  /*

  This works nicely you can watch the output on the screen.  You have to pre-deploy the jar file to each of the Spark workers

  /opt/spark/bin/spark-submit \
  --master spark://10.0.128.13:7077 \
  --conf spark.executor.extraClassPath="/home/spark/sparktest-jar-with-dependencies.jar" \
  --driver-class-path "/home/spark/sparktest-jar-with-dependencies.jar" \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  --conf spark.executor.memory=4000m \
  --conf spark.executor.cores=4 \
  --conf spark.cores.max=48 \
  --conf spark.streaming.concurrentJobs=64 \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.locality.wait=0s \
  --conf spark.streaming.kafka.consumer.cache.enabled=false \
  --conf spark.cassandra.output.batch.size.rows=auto \
  --conf spark.cassandra.output.concurrent.writes=200 \
  --class org.jennings.estest.KafkaToKafka \
  /home/spark/sparktest-jar-with-dependencies.jar broker.hub-gw01.l4lb.thisdcos.directory:9092 planes  broker.hub-gw01.l4lb.thisdcos.directory:9092 planes-out spark://10.0.128.13:7077 1


   */



  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 6) {
      System.err.println("Usage: KafkaToKafka fmBroker fmTopic toBroker toTopic ")
      System.err.println("        fmBroker: Server:IP")
      System.err.println("        fmTopic: Kafka Topic Name")
      System.err.println("        toBroker: Server:IP")
      System.err.println("        toTopic: Kafka Topic Name")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        Spark Streaming Interval Seconds (e.g. 1)")

      System.exit(1)

    }

    val fmBrokers = args(0)
    val fmTopic = args(1)
    val toBrokers = args(2)
    val toTopic = args(3)
    val spkMaster = args(4)
    val sparkStreamSeconds = args(5).toLong

    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> fmBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> java.util.UUID.randomUUID().toString(),
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array(fmTopic)

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(sparkStreamSeconds))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    val props = new Properties
    props.put("bootstrap.servers", toBrokers)
    props.put("client.id", getClass.getName)
    props.put("acks", "1")
    props.put("retries", new Integer(0))
    props.put("batch.size", new Integer(16384))
    props.put("linger.ms", new Integer(1))
    props.put("buffer.memory", new Integer(8192000))
    props.put("request.timeout.ms", "11000")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val kafkaSink = sc.broadcast(KafkaSink(props))

    stream.foreachRDD { rdd =>

      rdd.foreach { line =>

        kafkaSink.value.send(toTopic, line.key, line.value)

      }
    }

    ssc.start()

    ssc.awaitTermination()

  }
}
