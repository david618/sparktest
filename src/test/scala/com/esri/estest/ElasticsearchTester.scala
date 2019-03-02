package com.esri.estest

import org.jennings.estest.SendKafkaTopicElasticsearch
import org.scalatest.FunSuite

class ElasticsearchTester extends FunSuite {

  test("createIndex") {
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

  test("deleteIndex") {
    val esServer = "localhost"
    val esPort = "9200"
    val esUsername = "-"
    val esPassword = "-"
    val indexName = "planes4"

    val success = SendKafkaTopicElasticsearch.deleteIndex(esServer, esPort, esUsername, esPassword, indexName)
    assert(success == true)
  }

  test ("adaptCsvToPlane") {
    val csvLine = s"""0,1506957079575,240.25,5024.32,-70.72,1,"Mielec Airport","Frank Pais International Airport",-1,-31.88592,49.21297,AAA,BBB,CCC,DDD"""
    val json = SendKafkaTopicElasticsearch.adaptCsvToPlane(csvLine)
    assert(json != null)
  }

}