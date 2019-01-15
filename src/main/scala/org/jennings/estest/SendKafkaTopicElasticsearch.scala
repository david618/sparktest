package org.jennings.estest

import java.net.URL
import java.util.UUID

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.logging.LogFactory
import org.apache.http.client.methods.{CloseableHttpResponse, HttpDelete, HttpPut, HttpRequestBase, HttpUriRequest}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.Random


object SendKafkaTopicElasticsearch {

  private val log = LogFactory.getLog(this.getClass)

  // used to generate a random uuid
  private val RANDOM = new Random()

  val DEFAULT_TYPE_NAME = "_doc"


  // initialized the object mapper / text parser only once
  private val objectMapper = {
    // create an empty schema
    val schema = CsvSchema.emptySchema()
        .withColumnSeparator(',')
        .withLineSeparator("\\n")
    // create the mapper
    val csvMapper = new CsvMapper()
    csvMapper.enable(CsvParser.Feature.WRAP_AS_ARRAY)
    csvMapper
        .readerFor(classOf[Array[String]])
        .`with`(schema)
  }


  def main(args: Array[String]): Unit = {


    if (args.length < 13) {
      System.err.println("Usage: SendKafkaTopicElasticsearch [spkMaster] [emitIntervalMS] [kafkaBrokers] [kafkaConsumerGroup] [kafkaTopic] [kafkaThreads]" +
        "[elasticServer] [elasticPort] [elasticUsername] [elasticPassword] [elasticNumShards] [recreateTable] [debug] (<latest=true> <keyspace=realtime> <table=planes>)")
      System.exit(1)
    }

    // parse arguments
    //val Array(filename,esServer,esPort,spkMaster,indexAndType) = args
    val sparkMaster = args(0)
    val emitInterval = args(1).toLong
    val kBrokers = args(2)
    val kConsumerGroup = args(3)
    val kTopics = args(4)
    val kThreads = args(5)
    val esServer = args(6)
    val esPort = args(7)
    val esUsername = args(8)
    val esPassword = args(9)
    val esNumOfShards = args(10).toInt
    val recreateIndex = args(11).toBoolean
    val kDebug = args(12).toBoolean
    // optional params
    val kLatest = if (args.length > 13) args(13).toBoolean else true
    val esIndexName = if (args.length > 14) args(14) else "planes"


    // TODO - make these arguments?
    val replicationFactor: Int = 0
    val refreshInterval: String = "60s"
    val maxRecordCount: Int = 10000



    val sConf = new SparkConf(true)
      .set("es.index.auto.create", "true")
      .set("es.nodes", esServer)
      .set("es.port", esPort)
      // Without the following it would not create the index on single-node mode
      .set("es.nodes.discovery", "false")
      .set("es.nodes.data.only", "false")
      // Without setting es.nodes.wan.only the index was created but loading data failed (5.5.1)
      .set("es.nodes.wan.only", "true")
      .setAppName(getClass.getSimpleName)
      .set("es.net.http.auth.user", esUsername)
      .set("es.net.http.auth.pass", esPassword)

    val sc = new SparkContext(sparkMaster, "KafkaToElastic", sConf)



    // The following delete and create an Elasticsearch Index; otherwise it assumes index already exists
    if (recreateIndex) {
      println(s"We are recreating the index: $esIndexName")
      log.info(s"We are recreating the index: $esIndexName")
      deleteIndex(esServer, esPort, esUsername, esPassword, esIndexName)
      createIndex(esServer, esPort, esUsername, esPassword, esIndexName, replicationFactor, esNumOfShards, refreshInterval, maxRecordCount)
    }

    log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")

    // the streaming context
    val ssc = new StreamingContext(sc, Milliseconds(emitInterval))

    // resetToSt
    val resetToStr = if (kLatest) "latest" else "earliest"

    // create the kafka stream
    val stream = createKafkaStream(ssc, kBrokers, kConsumerGroup, kTopics, kThreads.toInt, resetToStr)

    // convert csv lines to ES JSON
    val dataStream = stream.map(line => csvToJson(line))

    // debug
    if (kDebug) {
      dataStream.foreachRDD {
        (rdd, time) =>
          val count = rdd.count()
          if (count > 0) {
            val msg = "Time %s: saving to DSE (%s total records)".format(time, count)
            log.warn(msg)
            println(msg)
          }
      }
    }

    dataStream.foreachRDD { rdd =>
//      rdd.foreach(a => {
//        println(a)
//      })
      EsSpark.saveJsonToEs(rdd, s"$esIndexName/$DEFAULT_TYPE_NAME")
    }

    log.info("Stream is starting now...")
    println("Stream is starting now...")

    // start the stream
    ssc.start
    ssc.awaitTermination()
  }

  /**
    * Adapt to the very specific Safegraph JSON Schema
    */
  private def csvToJson(csvLine: String): String = {
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](csvLine)
    val row = rows.nextValue()

    val id = uuid.toString // NOTE: This is to ensure unique records
    val ts = row(1).toLong
    val speed = row(2).toDouble
    val dist = row(3).toDouble
    val bearing = row(4).toDouble
    val rtid = row(5).toInt
    val orig = row(6)
    val dest = row(7)
    val secsToDep = row(8).toInt
    val longitude = row(9).toDouble
    val latitude = row(10).toDouble

    s"""{"id": "$id","ts": $ts,"speed": $speed,"dist": $dist,"bearing": $bearing,"rtid": $rtid,"orig": "$orig","dest": "$dest","secsToDep": $secsToDep,"longitude": $longitude,"latitude": $latitude,"geometry": [$longitude,$latitude]}"""
  }

  // create the kafka stream
  private def createKafkaStream(ssc: StreamingContext, brokers: String, consumerGroup: String, topics: String, numOfThreads: Int = 1, resetToStr: String): DStream[String] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroup,
      "auto.offset.reset" -> resetToStr,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topicMap = topics.split(",")
    val kafkaStreams = (1 to numOfThreads).map { i =>
      KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topicMap, kafkaParams)).map(_.value())
    }
    val unifiedStream = ssc.union(kafkaStreams)
    unifiedStream
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

  private def createIndex(esServer: String, esPort: String, username: String, password: String, indexName: String, replicationFactor: Int, numOfShards: Int, refreshInterval: String, maxRecordCount: Int): Boolean = {

    // create the index
    val indexJson = getIndexJson(indexName, replicationFactor, numOfShards, refreshInterval, maxRecordCount)
    //println(indexJson)
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

//    val indexJson =
//      s"""
//         |{
//         |  "settings": $indexSettingsJson,
//         |  "mappings": {
//         |    $indexMappingJson
//         |  },
//         |  "aliases": {
//         |    "$indexName": {}
//         |  }
//         |}
//     """.stripMargin
        val indexJson =
          s"""
             |{
             |  "settings": $indexSettingsJson,
             |  "mappings": {
             |    $indexMappingJson
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

