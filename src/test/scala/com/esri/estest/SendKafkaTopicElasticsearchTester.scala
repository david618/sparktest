package com.esri.estest

import org.jennings.estest.SendKafkaTopicElasticsearch


object SendKafkaTopicElasticsearchTester extends App {

  val sparkMaster = "local[8]"  // "spark://m1:7077"
  val emitIntervalMS = "1000"

  val kafkaBrokers = "a41:9092"
  val kafkaConsumerGroup = "group1"
  val kafkaTopic = "planes9"
  val kafkaThreads = "1"

  val esNodes = "localhost"  // "a104,a105,a106,a107,a108,a109,a110,a101,a102,a103"
  val esPort = "9200"
  val esUsername = "-"
  val esPassword = "-"
  val esNumOfShards = "1"
  val esRecreateIndex = true.toString

  val debug = true.toString
  val latest = true.toString

  val indexName = "planes"
  val refreshInterval = "60s"
  val maxRecordCount = "10000"
  val replicationFactor = "0"

  val parameters = Array(
    sparkMaster, emitIntervalMS,
    kafkaBrokers, kafkaConsumerGroup, kafkaTopic, kafkaThreads,
    esNodes, esPort, esUsername, esPassword, esNumOfShards,
    esRecreateIndex, debug,
    latest,
    indexName, refreshInterval, maxRecordCount, replicationFactor
  )

  SendKafkaTopicElasticsearch.main(parameters)
}
