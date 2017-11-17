package org.jennings.estest

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.JavaConversions._

object SendS3FilesElasticsearch {



  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 8 && numargs != 10) {
      System.err.println("Usage: FilesAws access-key secret-key bucket files ESServer ESPort SpkMaster IndexType Username Password")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        bucket: Bucket to List")
      System.err.println("        files: File Pattern")
      System.err.println("        ESServer: Elasticsearch Server Name or IP")
      System.err.println("        ESPort: Elasticsearch Port (e.g. 9200)")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")
      System.err.println("        IndexType: Index/Type (e.g. planes/events")
      System.err.println("        Username: Elasticsearch Username (optional)")
      System.err.println("        Password: Elasticsearch Password (optional)")
      System.exit(1)

    }

    val accessKey = args(0)
    val secretKey = args(1)
    val bucket_name = args(2)
    val pattern = args(3)
    val esServer = args(4)
    val esPort = args(5)
    val spkMaster = args(6)
    val indexAndType = args(7)



    val sparkConf = new SparkConf().setAppName(appName)
    if (!spkMaster.equalsIgnoreCase("-")) {
      sparkConf.setMaster(spkMaster)
    }
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", esServer)
    sparkConf.set("es.port", esPort)
    sparkConf.set("es.nodes.discovery", "false")
    sparkConf.set("es.nodes.data.only", "false")
    sparkConf.set("es.nodes.wan.only", "true")

    if (numargs == 10) {
      val username = args(8)
      val password = args(9)
      sparkConf.set("es.net.http.auth.user", username)
      sparkConf.set("es.net.http.auth.pass", password)
    }

    // Hardcoded to US_EAST_1 for now
    val awsCreds = new BasicAWSCredentials(accessKey, secretKey)
    val s3Client = AmazonS3ClientBuilder.standard
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.US_EAST_1)
      .build


    val sc = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.setInt("fs.s3a.connection.maximum",5000)

    val ol = s3Client.listObjects(bucket_name)
    val objects = ol.getObjectSummaries.filter(_.getKey().matches(pattern))


    for (os:S3ObjectSummary <- objects) {
      System.out.println("Processing " + os.getKey)
      val textFile = sc.textFile("s3a://" + bucket_name + "/" + os.getKey)

      EsSpark.saveJsonToEs(textFile, indexAndType)
    }

  }

}
