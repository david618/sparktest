package org.jennings.estest

import java.util.{Properties, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by david on 11/2/2018.
  *
  * java -cp target/sparktest.jar org.jennings.estest.SendFileKafka /home/centos/rttest/planes00001 broker.hub-gw01.l4lb.thisdcos.directory:9092 planes -
  *
  *
  *  The following didn't work....
  * ./spark/bin/spark-submit   --class org.jennings.estest.SendFileKafka
  *     --master spark://10.0.128.13:7077
  *     --deploy-mode cluster http://10.0.128.17/sparktest-jar-with-dependencies.jar /home/spark/planes00001 broker.hub-gw01.l4lb.thisdcos.directory:9092 planes -
  *
  *
  * This worked; had to preposition the jar file and planes00001
  *
  * /opt/spark/bin/spark-submit \
  * --master spark://10.0.128.13:7077 \
  * --conf spark.executor.extraClassPath="/home/spark/sparktest-jar-with-dependencies.jar" \
  * --driver-class-path "/home/spark/sparktest-jar-with-dependencies.jar" \
  * --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  * --conf spark.executor.extraJavaOptions=-Dlog4j.configurationFile=/home/spark/log4j2conf.xml \
  * --conf spark.executor.memory=4000m \
  * --conf spark.executor.cores=4 \
  * --conf spark.cores.max=48 \
  * --conf spark.streaming.concurrentJobs=64 \
  * --conf spark.scheduler.mode=FAIR \
  * --conf spark.locality.wait=0s \
  * --conf spark.streaming.kafka.consumer.cache.enabled=false \
  * --conf spark.cassandra.output.batch.size.rows=auto \
  * --conf spark.cassandra.output.concurrent.writes=200 \
  * --class org.jennings.estest.SendFileKafka \
  * /home/spark/sparktest-jar-with-dependencies.jar /home/spark/planes00001 broker.hub-gw01.l4lb.thisdcos.directory:9092 planes - 10
  *
  *
  *
  */
object SendFileKafka {

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 5) {
      System.err.println("Usage: SendFileKafka Filename Brokers Topic SpkMaster NumTimesSendFile")
      System.err.println("        Filename: JsonFile to Process")
      System.err.println("        Brokers: CSV list of Kafka Brokers")
      System.err.println("        Topic: Kafka Topic")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        NumTimesSendFile: Number of Times to Send the File")
      System.exit(1)

    }

    val filename = args(0)
    val brokers = args(1)
    val topic = args(2)
    val spkMaster = args(3)
    val numTimesSend = args(4)

    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.port.maxRetries", "50")
    if (spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster("local[8]")
    } else {
      sparkConf.setMaster(spkMaster)
    }

    println("Sending " + filename + " " + numTimesSend + " times to " + brokers + ":" + topic + " using " + spkMaster)

    val sc = new SparkContext(sparkConf)

    // These lines added; otherwise the big jar fails with error "No FileSystem for scheme: file"
    val hadoopConfig: Configuration = sc.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    val textFile = sc.textFile(filename)


    //println(textFile.count())

    val props = new Properties
    props.put("bootstrap.servers", brokers)
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

    for ( i <- 1 to numTimesSend.toInt ) {

      textFile.foreach { line =>

        val uuid = UUID.randomUUID
        kafkaSink.value.send(topic, uuid.toString, line)

      }
    }



  }
}



