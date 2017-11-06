package org.jennings.estest

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

/**
  * Created by david on 11/4/17.
  */
object SearchFileLinesContainingString {



  // Example call: java -cp target/estest.jar org.jennings.estest.SearchFileLinesContainingString ../Simulator/planes00001.json 00001


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 2) {
      System.err.println("Usage: SearchFileLinesContainingString Filename SearchString")
      System.err.println("        Filename: File to search")
      System.err.println("        SearchString: String search")
      System.exit(1)

    }

    val Array(filename, searchString) = args

    println("Find the number of line in file named " + filename + " that contain " + searchString + " ")

    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[8]")

    val sc = new SparkContext(sparkConf)

    val textFile =  sc.textFile(filename)

    // Search for patterns on lines and count them
    val numLines = textFile.filter(line => line.contains(searchString)).count()
    println("Lines with %s: %s".format(searchString, numLines))

  }
}
