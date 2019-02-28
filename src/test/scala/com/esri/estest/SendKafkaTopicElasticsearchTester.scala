package com.esri.estest

import org.jennings.estest.SendKafkaTopicElasticsearch


object SendKafkaTopicElasticsearchTester extends App {

  val sparkMaster = "local[8]"  // "spark://m1:7077"
  val emitIntervalMS = "1000"

  val kafkaBrokers = "a41:9092"
  val kafkaConsumerGroup = "group1"
  val kafkaTopic = "planes9"
  val kafkaThreads = "1"

  val esServer = "localhost"  // "a101"
  val esPort = "9200"
  val esUsername = ""
  val esPassword = ""

  val esNumOfShards = "1"

  val recreateTable = true.toString
  val debug = true.toString
  val latest = true.toString

  val indexName = "planes"
  val refreshInterval= "60s"
  val maxRecordCount="10000"
  val replicationFactor="0"

  val parameters = Array(
    sparkMaster, emitIntervalMS,
    kafkaBrokers, kafkaConsumerGroup, kafkaTopic, kafkaThreads,
    esServer, esPort, esUsername, esPassword, esNumOfShards,
    recreateTable, debug,
    latest,
    indexName, refreshInterval, maxRecordCount, replicationFactor
  )

  SendKafkaTopicElasticsearch.main(parameters)
}
