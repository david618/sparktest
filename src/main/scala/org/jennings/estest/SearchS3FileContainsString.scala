package org.jennings.estest

import org.apache.spark.{SparkConf, SparkContext}


object SearchS3FileContainsString {

  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 4) {
      System.err.println("Usage: SearchS3FileContainingString access-key secret-key filename SearchString")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        Filename: File to search")
      System.err.println("        SearchString: String search")
      System.exit(1)

    }

    val Array(accessKey, secretKey, filename, searchString) = args

    println("Find the number of lines in file named " + filename + " that contain " + searchString + " ")

    val sparkConf = new SparkConf().setAppName(appName).setMaster("local[64]")

    //sparkConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    //sparkConf.set("spark.hadoop.fs.s3a.access.key", accessKey)
    //sparkConf.set("spark.hadoop.fs.s3a.secret.key", secretKey)



    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.setInt("fs.s3a.connection.maximum",5000)

    val textFile =  sc.textFile(filename)

    // Search for patterns on lines and count them
    val numLines = textFile.filter(line => line.contains(searchString)).count()
    println("Lines with %s: %s".format(searchString, numLines))

  }

}
