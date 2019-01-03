package org.jennings.estest


import java.util.UUID

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.util.Random


object SendS3FilesCassandra {

  private val log = LogFactory.getLog(this.getClass)


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 10 && numargs != 9) {
      System.err.println("Usage: SendS3FilesCassandra [access-key] [secret-key] [bucket] [files] [cassandraHost] [replicationFactor] [recreateTable] [useSolr] [storeGeo] [Spark Master] ")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        bucket: Bucket to List")
      System.err.println("        files: File Pattern")
      System.err.println("        cassandraHost: Cassandra Server Name or IP")
      System.err.println("        replicationFactor: Cassandra Replication Factor")
      System.err.println("        recreateTable: Delete and create table")
      System.err.println("        useSolr: Add Search Index")
      System.err.println("        storeGeo: Convert Lat/Lon into Geometry")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8]; leave blank to use --master in spark-submit)")

      System.exit(1)

    }

    val accessKey = args(0)
    val secretKey = args(1)
    val bucket_name = args(2)
    val pattern = args(3)
    val cassandraHost = args(4)
    val replicationFactor = args(5)
    val recreateTable = args(6)
    val useSolr = args(7)
    val storeGeo = args(8)
    val spkMaster = if (numargs == 10) {
      args(9)
    } else {
      ""
    }


    // configuration
    val sConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.cassandra.output.consistency.level", ConsistencyLevel.ONE.toString)
      .setAppName(getClass.getSimpleName)

    if (numargs == 10) {
      sConf.setMaster(spkMaster)
    }

    val keyspace = "realtime"
    val table = "planes"

    // check if need to recreate the tables
    if (recreateTable.toBoolean) {
      log.info(s"We are recreating the table: $keyspace.$table")
      println(s"We are recreating the table: $keyspace.$table")
      CassandraConnector(sConf).withSessionDo {
        session =>
          session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")
          session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': $replicationFactor }")
          session.execute(s"DROP TABLE IF EXISTS $keyspace.$table")

          // FiXME: Dynamically create the CREATE TABLE sql based on schema
          session.execute(s"""
          CREATE TABLE IF NOT EXISTS $keyspace.$table
          (
            id bigint,
            ts timestamp,
            speed double,
            dist double,
            bearing double,
            rtid int,
            orig text,
            dest text,
            secstodep int,
            lon double,
            lat double,
            geometry text,
            PRIMARY KEY ((id), ts)
          ) WITH CLUSTERING ORDER BY (ts DESC)"""
          )

          if (useSolr.toBoolean) {
            // enable search on all fields (except geometry)
            session.execute(
              s"""
                 | CREATE SEARCH INDEX ON $keyspace.$table
                 | WITH COLUMNS
                 |  id,
                 |  ts,
                 |  speed,
                 |  dist,
                 |  bearing,
                 |  rtid,
                 |  orig,
                 |  dest,
                 |  secstodep,
                 |  lon,
                 |  lat
           """.stripMargin
            )

            // check if we want to store the Geo
            if (storeGeo.toBoolean) {
              // enable search on geometry field
              session.execute(
                s"""
                   |ALTER SEARCH INDEX SCHEMA ON $keyspace.$table
                   |ADD types.fieldType[ @name='rpt',
                   |                     @class='solr.SpatialRecursivePrefixTreeFieldType',
                   |                     @geo='false',
                   |                     @worldBounds='ENVELOPE(-1000, 1000, 1000, -1000)',
                   |                     @maxDistErr='0.001',
                   |                     @distanceUnits='degrees' ]
             """.stripMargin
              )
              session.execute(
                s"""
                   |ALTER SEARCH INDEX SCHEMA ON $keyspace.$table
                   |ADD fields.field[ @name='geometry',
                   |                  @type='rpt',
                   |                  @indexed='true',
                   |                  @stored='true' ];
             """.stripMargin
              )
              session.execute(
                s"RELOAD SEARCH INDEX ON $keyspace.$table"
              )
            }
          }
      }
    }

    log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")

    // Hardcoded to US_EAST_1 for now
    val awsCreds = new BasicAWSCredentials(accessKey, secretKey)
    val s3Client = AmazonS3ClientBuilder.standard
      .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
      .withRegion(Regions.US_EAST_1)
      .build


    val sc = new SparkContext(sConf)


    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    sc.hadoopConfiguration.setInt("fs.s3a.connection.maximum",5000)

    val ol = s3Client.listObjects(bucket_name)
    val objects = ol.getObjectSummaries.filter(_.getKey().matches(pattern))


    for (os:S3ObjectSummary <- objects) {
      System.out.println("Processing " + os.getKey)

      val textFile = sc.textFile("s3a://" + bucket_name + "/" + os.getKey).map(line => adaptSpecific(line))


      textFile.saveToCassandra(
        keyspace,
        table,
        SomeColumns(
          "id",
          "ts",
          "speed",
          "dist",
          "bearing",
          "rtid",
          "orig",
          "dest",
          "secstodep",
          "lon",
          "lat",
          "geometry"
        )
      )

    }



  }

  // used to generate a random uuid
  private val RANDOM = new Random()

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

  /**
    * Adapt to the very specific Safegraph Schema
    */
  private def adaptSpecific(line: String) = {

    //println(line)
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())

    // parse out the line
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()

    val id = row(0).toLong              // Use planes00001 with 1 million unique id's 0 to 999,999.
    //val ts = System.currentTimeMillis()  // Current time im ms from epoch; With 1,000,000 unique id's the combination of id/ts will unique even for rates of several 100 million per second
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
    val geometryText = "POINT (" + row(9) + " " + row(10) + ")"

    // FIXME: why do we need to convert to tuples? why cant we store the data as a map?
    val data = (id, ts, speed, dist, bearing, rtid, orig, dest, secsToDep, latitude, longitude, geometryText)
    //println(data)
    data
  }


}
