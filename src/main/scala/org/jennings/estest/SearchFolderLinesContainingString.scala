package org.jennings.estest

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by david on 11/4/17.
  */
object SearchFolderLinesContainingString {



  // Example call: java -cp target/estest.jar org.jennings.estest.SearchFileLinesContainingString ../Simulator ab


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 2) {
      System.err.println("Usage: SearchFileLinesContainingString Foldername SearchString")
      System.err.println("        Foldername: Folder with files you want to search")
      System.err.println("        SearchString: String search")
      System.exit(1)

    }

    val Array(foldername, searchString) = args

    println("Find the number of line in each file in the folder " + foldername + " that contain " + searchString + " ")

    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[8]")

    val sc = new SparkContext(sparkConf)

    val folder = new File(foldername)
    
    val files = folder.listFiles().iterator


    println("Number of lines containing: %s".format(searchString))
    while (files.hasNext) {
      val file = files.next
      val filename = foldername + File.separator + file.getName
      println(filename)
      val textFile =  sc.textFile(filename)

      // Search for patterns on lines and count them
      val numLines = textFile.filter(line => line.contains(searchString)).count()
      println("%s: %s".format(filename, numLines))

    }


  }
}
