package org.jennings.estest

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by david on 11/4/17.
  */
object ProcessFile {
  def main(args: Array[String]): Unit = {

    //Logging.setStreamingLogLevels()

    val appName = "ProcessFile"

    val numargs = args.length

    if (numargs != 1) {
      System.err.println("Usage: ProcessFile Filename")
      System.err.println("        Filename: JsonFile to Process")
      System.exit(1)

    }

    val Array(filename) = args

    println(filename)

    //val sparkConf = new SparkConf().setAppName(appName)

    //val sparkConf = new SparkConf().setAppName(appName).setMaster("local[8]").set("spark.executor.memory", "1g")
    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[8]")
    sparkConf.set("es.index.auto.create", "true")
    //sparkConf.set("es.cluster.name","elasticsearch")
    //sparkConf.set("spark.es.nodes","192.168.56.95")
    sparkConf.set("spark.es.nodes","e5")
    sparkConf.set("es.nodes.discovery", "false")
    sparkConf.set("es.nodes.data.only", "false")
    sparkConf.set("es.nodes.wan.only","true")


    //var esconf = Map("es.nodes" -> "192.168.56.95", "es.port" -> "9200")

    val sc = new SparkContext(sparkConf)

    val textFile =  sc.textFile("/home/david/github/Simulator/planes00001.json")

    //val numAs = textFile.filter(line => line.contains("aa")).count()
    //val numBs = textFile.filter(line => line.contains("ba")).count()
    //println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    //EsSpark.saveJsonToEs(textFile, "planes", esconf)
    EsSpark.saveJsonToEs(textFile, "planes/planes")


//    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
//    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
//
//    val myRDD = sc.makeRDD(Seq(json1, json2))
//    EsSpark.saveJsonToEs(myRDD, "spark/json-trips")


  }
}
