package org.jennings.estest

import java.util.{Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by david on 11/4/17.
  */
object SendFileKafka {

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 4) {
      System.err.println("Usage: SendFileKafka Filename Brokers Topic SpkMaster")
      System.err.println("        Filename: JsonFile to Process")
      System.err.println("        Brokers: CSV list of Kafka Brokers")
      System.err.println("        Topic: Kafka Topic")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.exit(1)

    }

    val filename = args(0)
    val brokers = args(1)
    val topic = args(2)
    val spkMaster = args(3)

    //val Array(filename,esServer,esPort,spkMaster,indexAndType) = args

    println("Sending " + filename + " to " + brokers + ":" + topic + " using " + spkMaster)

    val sparkConf = new SparkConf().setAppName(appName)
    sparkConf.set("spark.port.maxRetries", "50")
    if (spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster("local[8]")
    } else {
      sparkConf.setMaster(spkMaster)
    }


    val sc = new SparkContext(sparkConf)

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

    textFile.foreach { line =>

      val uuid = UUID.randomUUID
      kafkaSink.value.send(topic, uuid.toString, line)

    }




  }
}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  lazy val producer = createProducer()

  def send(topic: String, value: String): Unit = producer.send(new ProducerRecord(topic, value))
  def send(topic: String, key: String, value: String): Unit = producer.send(new ProducerRecord(topic, key, value))

}


object KafkaSink {
  def apply(props: java.util.Properties): KafkaSink = {
    val f = () => {
      val producer = new KafkaProducer[String, String](props)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }
    new KafkaSink(f)
  }
}

