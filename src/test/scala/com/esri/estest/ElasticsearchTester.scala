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
    val indexHashFields = false

    val success = SendKafkaTopicElasticsearch.createIndex(esServer, esPort, esUsername, esPassword, indexName, replicationFactor, esNumOfShards, refreshInterval, maxRecordCount, indexHashFields)
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
    val csvLine1 = s"""0,1506957079575,240.25,5024.32,-70.72,1,"Mielec Airport","Frank Pais International Airport",-1,-31.88592,49.21297,AAA,BBB,CCC,DDD"""
    val json1 = SendKafkaTopicElasticsearch.adaptCsvToPlane(csvLine1)
    assert(json1 != null)

    val csvLine2 = s"""1,1547480515000,226.92,7443.13,-20.89,2,"Cozumel International Airport","Phnom Penh International Airport",-1,160.948738273412,67.805666315828,zs43wrxk9kmd,ACADDCCACCADABBADDCCBABDDBBBDA,CGHHHHGHHHEGFCHEHHHGGHFABCGHGH,BCFBCGFBBACFBBCFBCGEEGFABBBCEG"""
    val json2 = SendKafkaTopicElasticsearch.adaptCsvToPlane(csvLine2)
    assert(json2 != null)
  }

  test ("parseEsServer") {
    val esNodesMultiple = "a104,a105,a106,a107,a108,a109,a110,a101,a102,a103"
    val esServer1 = SendKafkaTopicElasticsearch.parseEsServer(esNodesMultiple)
    assert(esServer1.equals("a104"))

    val esNodesSingle = "a104"
    val esServer2 = SendKafkaTopicElasticsearch.parseEsServer(esNodesSingle)
    assert(esServer2.equals("a104"))
  }
}