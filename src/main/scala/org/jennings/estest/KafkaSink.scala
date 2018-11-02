package org.jennings.estest

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


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