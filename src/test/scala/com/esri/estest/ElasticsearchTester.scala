package com.esri.estest

import org.jennings.estest.SendKafkaTopicElasticsearch
import org.scalatest.FunSuite

class ElasticsearchTester extends FunSuite {

  test("test create index") {
    val esServer = "localhost"
    val esPort = "9200"
    val esUsername = "-"
    val esPassword = "-"
    val indexName = "planes4"
    val replicationFactor = 0
    val esNumOfShards = 1
    val refreshInterval = "1s"
    val maxRecordCount = 10000

    val success = SendKafkaTopicElasticsearch.createIndex(esServer, esPort, esUsername, esPassword, indexName, replicationFactor, esNumOfShards, refreshInterval, maxRecordCount)
    assert(success == true)
  }

}