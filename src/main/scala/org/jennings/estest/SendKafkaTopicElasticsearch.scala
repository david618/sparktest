package org.jennings.estest

import java.net.URL

import org.apache.commons.codec.binary.Base64
import org.apache.http.client.methods.{CloseableHttpResponse, HttpDelete, HttpPut, HttpRequestBase, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark


object SendKafkaTopicElasticsearch {

  val DEFAULT_TYPE_NAME = "_doc"

  // TODO - make these arguments?
  val replicationFactor: Int = 0
  val numOfShards: Int = 5
  val refreshInterval: String = "60s"
  val maxRecordCount: Int = 10000


  // spark-submit --class org.jennings.sparktest.SendKafkaTopicElasticsearch target/sparktest.jar

  // java -cp target/sparktest.jar org.jennings.estest.SendKafkaTopicElasticsearch

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 8 && numargs != 10) {
      System.err.println("Usage: SendKafkaTopicElasticsearch broker topic ESServer ESPort SpkMaster StreamingIntervalSec indexName recreateIndex (Username) (Password)")
      System.err.println("        broker: Server:IP")
      System.err.println("        topic: Kafka Topic Name")
      System.err.println("        ESServer: Elasticsearch Server Name or IP")
      System.err.println("        ESPort: Elasticsearch Port (e.g. 9200)")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        Spark Streaming Interval Seconds (e.g. 1)")
      System.err.println("        IndexName: Index name (e.g. planes)")
      System.err.println("        recreateIndex: whether to recreate the index")
      System.err.println("        Username: Elasticsearch Username (optional)")
      System.err.println("        Password: Elasticsearch Password (optional)")
      System.exit(1)
    }

    // parse arguments
    //val Array(filename,esServer,esPort,spkMaster,indexAndType) = args
    val broker = args(0)
    val topic = args(1)
    val esServer = args(2)
    val esPort = args(3)
    val spkMaster = args(4)
    val sparkStreamSeconds = args(4).toLong
    val indexName = args(5)
    val recreateIndex = args(6).toBoolean
    val (username, password) = {
      if (numargs == 10) {
        (args(8), args(9))
      } else {
        (null, null)
      }
    }

    println("Sending " + topic + " to " + esServer + ":" + esPort + " using " + spkMaster)

    createIndex(esServer, esPort, username, password, indexName, recreateIndex, replicationFactor, numOfShards, refreshInterval, maxRecordCount)

    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", esServer)
    sparkConf.set("es.port", esPort)
    // Without the following it would not create the index on single-node mode
    sparkConf.set("es.nodes.discovery", "false")
    sparkConf.set("es.nodes.data.only", "false")
    // Without setting es.nodes.wan.only the index was created but loading data failed (5.5.1)
    sparkConf.set("es.nodes.wan.only", "true")

    if (username != null && password != null) {
      sparkConf.set("es.net.http.auth.user", args(8))
      sparkConf.set("es.net.http.auth.pass", args(9))

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

    stream.foreachRDD { rdd =>

      //rdd.map(_.value())

      val tsRDD = rdd.map(_.value)
      EsSpark.saveJsonToEs(tsRDD, indexName)

      // Error KafkaConsumer is not thread safe errors when trying the following

      //      val thread = new Thread {
      //        override def run: Unit = {
      //          EsSpark.saveJsonToEs(tsRDD, indexAndType)
      //        }
      //      }
      //      thread.start
      //      println(rdd.count());
      //      rdd.foreach { f =>
      //        println(f.value())
      //      }
    }

    ssc.start()

    ssc.awaitTermination()
  }

  private def deleteIndex(esServer: String, esPort: String, username: String, password: String, indexName: String): Boolean = {
    val client = HttpClients.createDefault()
    try {
      val urlStr = s"http://$esServer:$esPort/$indexName"
      val url: URL = new URL(urlStr)
      val httpDelete: HttpDelete = new HttpDelete(url.toURI)
      addAuthorizationHeader(httpDelete, username, password)
      val deleteResponse = client.execute(httpDelete)
      val deleteResponseAsString = getHttpResponseAsString(deleteResponse, httpDelete)
      deleteResponseAsString match {
        case HttpResponse(HttpStatus.SC_OK, bulkResponseStr) =>
          // do nothing - all is well
          true

        case HttpResponse(code, httpErrorMsg) =>
          false
      }
    } catch {
      case error: Exception =>
        false
    } finally {
      client.close()
    }
  }

  private def createIndex(esServer: String, esPort: String, username: String, password: String, indexName: String, recreate: Boolean, replicationFactor: Int, numOfShards: Int, refreshInterval: String, maxRecordCount: Int): Boolean = {
    if (recreate) {
      // delete the index
      deleteIndex(esServer, esPort, username, password, indexName)
    }

    // create the index
    val indexJson = getIndexJson(indexName, replicationFactor, numOfShards, refreshInterval, maxRecordCount)
    val client = HttpClients.createDefault()
    try {
      // re-create the template
      val urlStr = s"http://${esServer}:${esPort}/$indexName"
      val url: URL = new URL(urlStr)
      val httpPut: HttpPut = new HttpPut(url.toURI)
      addAuthorizationHeader(httpPut, username, password)
      httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
      httpPut.setHeader("charset", "utf-8")
      httpPut.setEntity(new StringEntity(indexJson, ContentType.APPLICATION_JSON))

      val createResponse = client.execute(httpPut)
      val createResponseAsString = getHttpResponseAsString(createResponse, httpPut)
      createResponseAsString match {
        case HttpResponse(HttpStatus.SC_OK, _) =>
          // proceed, all is well
          true

        case HttpResponse(code, httpErrorMsg) =>
          false
      }
    } catch {
      case error: Exception =>
        false
    } finally {
      client.close()
    }
  }

  private def getIndexJson(indexName: String, replicationFactor: Int, numOfShards: Int, refreshInterval: String, maxRecordCount: Int): String = {
    val indexSettingsJson =
      s"""
         |{
         |  "index": {
         |    "number_of_shards": "$numOfShards",
         |    "number_of_replicas": "$replicationFactor",
         |    "refresh_interval": "$refreshInterval",
         |    "max_result_window": $maxRecordCount
         |  }
         |}
     """.stripMargin

    val indexMappingJson =
      s"""
         |"$DEFAULT_TYPE_NAME": {
         |  "properties": {
         |    "id": {
         |      "type": "keyword"
         |    },
         |    "ts": {
         |      "type": "date",
         |      "format": "epoch_millis"
         |    },
         |    "speed": {
         |      "type": "double"
         |    },
         |    "dist": {
         |      "type": "double"
         |    },
         |    "bearing": {
         |      "type": "double"
         |    },
         |    "rtid": {
         |      "type": "integer"
         |    },
         |    "orig": {
         |      "type": "keyword"
         |    },
         |    "dest": {
         |      "type": "keyword"
         |    },
         |    "secstodep": {
         |      "type": "integer"
         |    },
         |    "lon": {
         |      "type": "double"
         |    },
         |    "lat": {
         |      "type": "double"
         |    },
         |    "geometry": {
         |      "type": "geo_point"
         |    }
         |  }
         |}
    """.stripMargin

    val indexJson =
      s"""
         |{
         |  "settings": $indexSettingsJson,
         |  "mappings": {
         |    $indexMappingJson
         |  },
         |  "aliases": {
         |    "$indexName": {}
         |  }
         |}
     """.stripMargin

    indexJson
  }

  private def getHttpResponseAsString(response: CloseableHttpResponse, request: HttpUriRequest): HttpResponse = {
    // make sure we close the response
    try {
      val entity = response.getEntity
      val responseString = {
        if (entity != null) {
          EntityUtils.toString(entity, "UTF-8")
        } else {
          ""
        }
      }
      HttpResponse(response.getStatusLine.getStatusCode, responseString)
    } finally {
      response.close()
    }
  }

  private def addAuthorizationHeader(httpRequest: HttpRequestBase, username: String, password: String): Unit = {
    // credentials
    if (username != null && password != null) {
      val authString = username + ":" + password
      val credentialString: String = Base64.encodeBase64String(authString.getBytes)
      httpRequest.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + credentialString)
    }
  }
}

/**
  * Helper Case Class, represents an HTTP Response
  *
  * @param code    the http code (200, 401, 500, etc)
  * @param message the http entity message as a string
  */
case class HttpResponse(code: Int, message: String) extends Serializable

