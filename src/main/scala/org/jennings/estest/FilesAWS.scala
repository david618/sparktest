package org.jennings.estest

import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import collection.JavaConversions._

object FilesAWS {



  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 4) {
      System.err.println("Usage: FilesAws access-key secret-key bucket files")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        bucket: Bucket to List")
      System.err.println("        files: File Pattern")
      System.exit(1)

    }

    val accessKey = args(0)
    val secretKey = args(1)
    val bucket_name = args(2)
    val pattern = args(3)

    //System.out.println(accessKey)
    //System.out.println(secretKey)
    //System.out.println(bucket_name)

    val awsCreds = new BasicAWSCredentials(accessKey, secretKey)
    val s3Client = AmazonS3ClientBuilder.standard
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
        .withRegion(Regions.US_EAST_1)
      .build



    val ol = s3Client.listObjects(bucket_name)
    val objects = ol.getObjectSummaries.filter(_.getKey().matches(pattern))

//    for (os:S3ObjectSummary <- objects) {
//      System.out.println("* " + os.getKey)
//    }

    for (os:S3ObjectSummary <- objects) {
      System.out.println("* " + os.getKey)
    }

  }

}
