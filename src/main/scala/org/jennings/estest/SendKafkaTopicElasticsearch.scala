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
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.util.Random


object SendKafkaTopicElasticsearch {

  private val log = LogFactory.getLog(this.getClass)

  // used to generate a random uuid
  private val RANDOM = new Random()

  val DEFAULT_TYPE_NAME = "_doc"

  // TODO - make these arguments?
  private val replicationFactor: Int = 0
  private val numOfShards: Int = 5
  private val refreshInterval: String = "60s"
  private val maxRecordCount: Int = 10000

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

    //TODO - kafka arguments...
    val kDebug = args(11).toBoolean
    val kBrokers = args(12)
    val kConsumerGroup = args(13)
    val kTopics = args(14)
    val kThreads = args(15)
    val kLatest = if (args.length > 16) args(16).toBoolean else true

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

    // resetToSt
    val resetToStr = if (kLatest) "latest" else "earliest"

    // create the kafka stream
    //val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
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
      //val tsRDD = rdd.map(_.value)
      EsSpark.saveJsonToEs(rdd, indexName)
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

    s"""
       |{
       |   "id": "$id",
       |   "ts": $ts,
       |   "speed": $speed,
       |   "dist": $dist,
       |   "bearing": $bearing,
       |   "rtid": $rtid,
       |   "orig": "$orig",
       |   "dest": "$dest",
       |   "secsToDep": $secsToDep,
       |   "longitude": $longitude,
       |   "latitude": $latitude,
       |   "Geometry": [$longitude,$latitude]
       |}
     """.stripMargin
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

