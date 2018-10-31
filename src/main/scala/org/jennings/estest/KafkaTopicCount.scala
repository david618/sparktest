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
  --class org.jennings.estest.KafkaTopicCount \
  /home/spark/sparktest-jar-with-dependencies.jar broker.hub-gw01.l4lb.thisdcos.directory:9092 planes spark://10.0.128.13:7077 1


   */


  /*

  This uses all the resources of the Spark cluster; it works but you'll need to access the Spark UI to see what's going on.  You can tunnel to the
  Spark master port 8080 or 8081 (I deployed on a DC/OS master and 8080 was already used so Spark automatically started on 8081).  Tunneling
  doesn't work great; many of the links in the web ui are reference the spark workers and don't work.  Better solution is to install XWindows on
  one of the cluster nodes and access the Spark Web UI via that node. (e.g. VNC).


  /opt/spark/bin/spark-submit \
  --class org.jennings.estest.KafkaTopicCount \
  --master spark://10.0.128.13:7077 \
  --deploy-mode cluster http://10.0.128.17/sparktest-jar-with-dependencies.jar broker.hub-gw01.l4lb.thisdcos.directory:9092 planes spark://10.0.128.13:7077 1




   */


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
