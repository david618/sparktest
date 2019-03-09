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

  //private val log = LogFactory.getLog(getClass)
  private var objectId = 0

  // used to generate a random uuid
  private val RANDOM = new Random()

  // constants
  val DEFAULT_TYPE_NAME = "_doc"

  var logOnce = true

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
      System.err.println(
        "Usage: SendKafkaTopicElasticsearch" +
            " [spkMaster] [emitIntervalMS]" +                                                         // 0-1
            " [kafkaBrokers] [kafkaConsumerGroup] [kafkaTopic] [kafkaThreads]" +                      // 2-5
            " [esNodes] [esPort] [esUsername] [esPassword] [esNumOfShards] [esRecreateIndex] " +      // 6-11
            " [debug]" +                                                                              // 12
            " <latest=true>" +                                                                        // 13
            " <indexName=planes> <refreshInterval=60s> <maxRecordCount=10000> <replicationFactor=0>") // 14-17
      System.exit(1)
    }

    // parse arguments
    val sparkMaster: String = args(0)
    val emitInterval: Long = args(1).toLong

    val kBrokers: String = args(2)
    val kConsumerGroup: String = args(3)
    val kTopics: String = args(4)
    val kThreads: String = args(5)

    val esNodes: String = args(6)
    val esPort: String = args(7)
    val esUsername: String = args(8)
    val esPassword: String = args(9)
    val esNumOfShards: Int = args(10).toInt
    val esRecreateIndex: Boolean = args(11).toBoolean

    val kDebug: Boolean = args(12).toBoolean

    // optional arguments
    val kLatest: Boolean = if (args.length > 13) args(13).toBoolean else true

    val indexName: String = if (args.length > 14) args(14) else "planes"
    val refreshInterval: String = if (args.length > 15) args(15) else "60s"
    val maxRecordCount: Int = if (args.length > 16) args(16).toInt else 10000
    val replicationFactor: Int = if (args.length > 17) args(17).toInt else 0


    val sConf = new SparkConf(true)
        .setAppName(getClass.getSimpleName)
        .set("es.nodes", esNodes)
        .set("es.port", esPort)
        .set("es.net.http.auth.user", esUsername)
        .set("es.net.http.auth.pass", esPassword)

      //.set("es.index.auto.create", "true")

      // without the following it would not create the index on single-node mode
      //.set("es.nodes.discovery", "false")
      //.set("es.nodes.data.only", "false")

      // without setting es.nodes.wan.only the index was created but loading data failed (5.5.1)
      //.set("es.nodes.wan.only", "true")


    val sc = new SparkContext(sparkMaster, "KafkaToElastic", sConf)

    println(s"*** Running spark with config:")
    sConf.getAll.foreach(println)
    println(s"***")


    // The following deletes and creates an Elasticsearch Index, otherwise assumes the index already exists
    if (esRecreateIndex) {
      val esServer = parseEsServer(esNodes)
      println(s"Recreating the index '$indexName' using ES node '$esServer' ...")
      //log.info(s"Recreating the index '$indexName' using ES node '$esServer' ...")
      deleteIndex(esServer, esPort, esUsername, esPassword, indexName)
      createIndex(esServer, esPort, esUsername, esPassword, indexName, replicationFactor, esNumOfShards, refreshInterval, maxRecordCount)
    }


    //log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")

    // the streaming context
    val ssc = new StreamingContext(sc, Milliseconds(emitInterval))

    // resetToSt
    val resetToStr = if (kLatest) "latest" else "earliest"

    // create the kafka stream
    val stream = createKafkaStream(ssc, kBrokers, kConsumerGroup, kTopics, kThreads.toInt, resetToStr)

    // convert csv lines to ES JSON
    val dataStream = stream.map(line => adaptCsvToPlane(line))

    // debug
    if (kDebug) {
      dataStream.foreachRDD {
        (rdd, time) =>
          val count = rdd.count()
          if (count > 0) {
            val msg = "Time %s: saving to ES (%s total records)".format(time, count)
            //log.warn(msg)
            println(msg)
          }
      }
    }

    dataStream.foreachRDD { rdd =>
      try {

        // rdd.foreach(a => {
        //   println(a)
        // })
        EsSpark.saveJsonToEs(rdd, s"$indexName/$DEFAULT_TYPE_NAME")
      } catch {
        case error: Throwable =>
          println(s"***** rdd.saveJsonToEs($indexName/$DEFAULT_TYPE_NAME) caught the following exception: *****")
          error.printStackTrace()
          println(s"***** dropped the current rdd.saveJsonToEs batch (count: ${rdd.count}) and continue with the next batch... *****")
      }
    }

    //log.info("Stream is starting now...")
    println("Stream is starting now...")

    // start the stream
    ssc.start
    ssc.awaitTermination()
  }

  def adaptCsvToPlane(csvLine: String): String = {
    objectId = objectId + 1
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](csvLine)
    val row = rows.nextValue()

    val globalid = uuid.toString
    val planeid = row(0)
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
    //val geohashEnconding = row(11)
    val squareEncoding = row(12)
    val pointyTriangleEncoding = row(13)
    val flatTriangleEncoding = row(14)

    val esGeoPoint = s"""[$longitude,$latitude]"""

    s"""{"objectid": $objectId,"globalid": "$globalid","planeid": "$planeid","ts": $ts,"speed": $speed,"dist": $dist,"bearing": $bearing,"rtid": $rtid,"orig": "$orig","dest": "$dest","secsToDep": $secsToDep,"longitude": $longitude,"latitude": $latitude,"square": "$squareEncoding","pointy": "$pointyTriangleEncoding","flat": "$flatTriangleEncoding","geometry": $esGeoPoint}"""
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

  def parseEsServer(esNodes: String): String = {
    val endIndex = esNodes.indexOf(",")
    if (endIndex == -1) {
      esNodes
    } else {
      esNodes.substring(0, endIndex)
    }
  }

  def deleteIndex(esServer: String, esPort: String, username: String, password: String, indexName: String): Boolean = {
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

  def createIndex(esServer: String, esPort: String, username: String, password: String, indexName: String, replicationFactor: Int, numOfShards: Int, refreshInterval: String, maxRecordCount: Int): Boolean = {

    val aliasName = s"${indexName}_alias"
    val indexJson = getIndexJson(indexName, aliasName, replicationFactor, numOfShards, refreshInterval, maxRecordCount)
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

  private def getDataIndexMappingJson(): String = {
    val indexMappingJson =
      s"""
         |"$DEFAULT_TYPE_NAME": {
         |  "properties": {
         |    "globalid": {
         |      "type": "keyword"
         |    },
         |    "objectid": {
         |      "type": "long"
         |    },
         |    "planeid": {
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
         |    "square": {
         |      "type": "keyword"
         |    },
         |    "pointy": {
         |      "type": "keyword"
         |    },
         |    "flat": {
         |      "type": "keyword"
         |    },
         |    "geometry": {
         |      "type": "geo_point"
         |    }
         |  }
         |}
     """.stripMargin

    indexMappingJson
  }

  private def getIndexJson(indexName: String, aliasName: String, replicationFactor: Int, numOfShards: Int, refreshInterval: String, maxRecordCount: Int): String = {
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

    val indexMappingJson = getDataIndexMappingJson()

    val indexJson =
      s"""
         |{
         |  "settings": $indexSettingsJson,
         |  "mappings": {
         |    $indexMappingJson
         |  },
         |  "aliases": {
         |    "$aliasName": {}
         |  }
         |}
      """.stripMargin

    indexJson
  }

  private def getTemplateMappingJson(aliasName: String,
                                     templateIndexPattern: String,
                                     indexSettingsJson: String,
                                     indexMappingJson: String): String = {

    // create the index template (must be lower cased)
    s"""
       |{
       |  "index_patterns": ["$templateIndexPattern"],
       |  "settings": $indexSettingsJson,
       |  "mappings": {
       |    $indexMappingJson
       |  },
       |  "aliases": {
       |    "$aliasName": {}
       |  }
       |}
    """.stripMargin
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
