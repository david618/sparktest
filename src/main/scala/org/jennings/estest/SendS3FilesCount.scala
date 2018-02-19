package org.jennings.estest

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object SendS3FilesCount {



  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 5) {
      System.err.println("Usage: FilesAws access-key secret-key bucket files SpkMaster")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        bucket: Bucket to List")
      System.err.println("        files: File Pattern")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.exit(1)

    }

    val accessKey = args(0)
    val secretKey = args(1)
    val bucket_name = args(2)
    val pattern = args(3)
    val spkMaster = args(4)



    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }


    // Hardcoded to US_EAST_1 for now
    val awsCreds = new BasicAWSCredentials(accessKey, secretKey)
    val s3Client = AmazonS3ClientBuilder.standard
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.US_WEST_1)
      .build

    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.setInt("fs.s3a.connection.maximum",5000)

    val ol = s3Client.listObjects(bucket_name)
    val objects = ol.getObjectSummaries.filter(_.getKey().matches(pattern))

    var cnt = 0L;

    for (os:S3ObjectSummary <- objects) {
      System.out.println("Processing " + os.getKey)
      val textFile = sc.textFile("s3a://" + bucket_name + "/" + os.getKey)

      val fcnt = textFile.count()
      cnt += fcnt
    }

    System.out.println("Total: + " + cnt);

  }

}
