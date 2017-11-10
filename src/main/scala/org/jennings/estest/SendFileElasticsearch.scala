package org.jennings.estest

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by david on 11/4/17.
  */
object SendFileElasticsearch {

  // spark-submit --class org.jennings.estest.SendFileElasticsearch target/estest.jar planes00001 a1 9200 local[16] planes/planes

  // java -cp target/estest.jar org.jennings.estest.SendFileElasticsearchFile

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 5 && numargs != 7) {
      System.err.println("Usage: SendFileElasticsearchFile Filename ESServer ESPort SpkMaster")
      System.err.println("        Filename: JsonFile to Process")
      System.err.println("        ESServer: Elasticsearch Server Name or IP")
      System.err.println("        ESPort: Elasticsearch Port (e.g. 9200)")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        IndexType: Index/Type (e.g. planes/events")
      System.err.println("        Username: Elasticsearch Username (optional)")
      System.err.println("        Password: Elasticsearch Password (optional)")
      System.exit(1)

    }

    val Array(filename, esServer, esPort, spkMaster, indexAndType,username,password) = args


    println("Sending " + filename + " to " + esServer + ":" + esPort + " using " + spkMaster)

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

    if (numargs == 7) {
      sparkConf.set("es.net.http.auth.user", username)
      sparkConf.set("es.net.http.auth.pass", password)
    }


    val sc = new SparkContext(sparkConf)

    val textFile = sc.textFile(filename)

    EsSpark.saveJsonToEs(textFile, indexAndType)

  }
}
