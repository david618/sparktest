package org.jennings.estest

import java.io.InputStream
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.collection.JavaConversions._
import scala.util.Random

object SendS3FilesTimescale {
  private val log = LogFactory.getLog(this.getClass)


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs != 11 && numargs != 10) {
      System.err.println("Usage: SendS3FilesTimescale [access-key] [secret-key] [bucket] [files] [timescaleHost] [recreateTable] [indexName] [tableName] [indexFields] [chunkInterval] [Spark Master] ")
      System.err.println("        access-key: aws access key")
      System.err.println("        secret-key: aws secret key")
      System.err.println("        bucket: Bucket to List")
      System.err.println("        files: File Pattern")
      System.err.println("        timescaleHost: Timescale Server Name or IP")
      System.err.println("        recreateTable: Delete and create table")
      System.err.println("        schemaName: Schema to create table in")
      System.err.println("        tableName: Name of table")
      System.err.println("        indexFields: create an index for each field")
      System.err.println("        chunkInterval: HyperTable chunk time interval (ms).  Use 0 to accept default of 7 days")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8]; leave blank to use --master in spark-submit)")

      System.exit(1)

    }

    val accessKey = args(0)
    val secretKey = args(1)
    val bucket_name = args(2)
    val pattern = args(3)
    val timescaleHost = args(4)
    val recreateTable = args(5)
    val schema = args(6)
    val table = args(7)
    val indexFields = args(8)
    val chunkInterval = args(9)
    val spkMaster = if (numargs == 11) {
      args(10)
    } else {
      ""
    }


    // configuration
    val sConf = new SparkConf(true)
        .setAppName(getClass.getSimpleName)

    if (numargs == 8) {
      sConf.setMaster(spkMaster)
    }



    val url = s"jdbc:postgresql://$timescaleHost:5432/$schema"
    val properties = new Properties
    properties.put("user", "realtime")
    properties.put("password", "esri.test")

    // check if need to recreate the tables
    if (recreateTable.toBoolean) {
      log.info(s"We are recreating the table: $schema.$table")
      println(s"We are recreating the table: $schema.$table")
      classOf[org.postgresql.Driver]
      val connection = DriverManager.getConnection(url, properties)

      val statement = connection.createStatement()

      statement.execute(s"CREATE SCHEMA IF NOT EXISTS $schema")
      statement.execute(s"DROP TABLE IF EXISTS $schema.$table")

      // FiXME: Dynamically create the CREATE TABLE sql based on schema
      statement.execute(s"""
      CREATE TABLE IF NOT EXISTS $schema.$table
      (
        objectid    BIGSERIAL,
        id          UUID,
        ts          TIMESTAMP NOT NULL,
        speed       DOUBLE PRECISION,
        dist        DOUBLE PRECISION,
        bearing     DOUBLE PRECISION,
        rtid        INTEGER,
        orig        TEXT,
        dest        TEXT,
        secstodep   INTEGER,
        lon         DOUBLE PRECISION,
        lat         DOUBLE PRECISION,
        geohash     TEXT,
        sqrhash     TEXT,
        pntytrihash TEXT,
        flattrihash TEXT,
        geometry    GEOMETRY(POINT, 4326)
      )""".stripMargin
      )

      if(indexFields.toBoolean){
//        statement.execute(s"create index on $schema.$table (xxxx, ts DESC)")  //low cardinality
//        statement.execute(s"create index on $schema.$table (ts DESC, XXXX)") //range queries

        statement.execute(s"create index on $schema.$table (ts DESC, speed)")
        statement.execute(s"create index on $schema.$table (ts DESC, dist)")
        statement.execute(s"create index on $schema.$table (ts DESC, bearing)")
        statement.execute(s"create index on $schema.$table (rtid, ts DESC)")
        statement.execute(s"create index on $schema.$table (orig, ts DESC)")
        statement.execute(s"create index on $schema.$table (dest, ts DESC)")
        statement.execute(s"create index on $schema.$table (ts DESC, secstodep)")
        statement.execute(s"create index on $schema.$table (ts DESC, lon)")
        statement.execute(s"create index on $schema.$table (ts DESC, lat)")
        statement.execute(s"create index on $schema.$table (geohash, ts DESC)")
        statement.execute(s"create index on $schema.$table (sqrhash, ts DESC)")
        statement.execute(s"create index on $schema.$table (pntytrihash, ts DESC)")
        statement.execute(s"create index on $schema.$table (flattrihash, ts DESC)")
        statement.execute(s"create index on $schema.$table using GIST(geometry)")
      }

      statement.execute(s"select create_hypertable('$schema.$table', 'ts', chunk_time_interval => $chunkInterval)")

      if(indexFields.toBoolean){
        statement.execute(s"create index on $schema.$table (xxxx, ts DESC)")  //low cardinality
        statement.execute(s"create index on $schema.$table (ts DESC, XXXX)") //range queries

        statement.execute(s"create index on $schema.$table (ts DESC, speed)")
        statement.execute(s"create index on $schema.$table (ts DESC, dist)")
        statement.execute(s"create index on $schema.$table (ts DESC, bearing)")
        statement.execute(s"create index on $schema.$table (rtid, ts DESC)")
        statement.execute(s"create index on $schema.$table (orig, ts DESC)")
        statement.execute(s"create index on $schema.$table (dest, ts DESC)")
        statement.execute(s"create index on $schema.$table (ts DESC, secstodep)")
        statement.execute(s"create index on $schema.$table (ts DESC, lon)")
        statement.execute(s"create index on $schema.$table (ts DESC, lat)")
        statement.execute(s"create index on $schema.$table (geohash, ts DESC)")
        statement.execute(s"create index on $schema.$table (sqrhash, ts DESC)")
        statement.execute(s"create index on $schema.$table (pntytrihash, ts DESC)")
        statement.execute(s"create index on $schema.$table (flattrihash, ts DESC)")
        statement.execute(s"create index on $schema.$table using GIST(geometry)")
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
    val objects = ol.getObjectSummaries.filter(_.getKey.matches(pattern))

    //using COPY command so timestamps have to be formatted as dates
    val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    for (os:S3ObjectSummary <- objects) {
      System.out.println("Processing " + os.getKey)

      val textFile = sc.textFile("s3a://" + bucket_name + "/" + os.getKey).map(line => adaptSpecific(line, simpleFormat))


      // save to timescale
      textFile.foreachPartition( iterator => {
        classOf[org.postgresql.Driver]
        val connection = DriverManager.getConnection(url, properties)
        val copyManager = new CopyManager(connection.asInstanceOf[BaseConnection])
        val copySql = s"""COPY $schema.$table (globalid,ts,speed,dist,bearing,rtid,orig,dest,secstodep,lon,lat,geometry ) FROM STDIN WITH (NULL 'null', FORMAT CSV, DELIMITER ',')"""

        val rowsCopied = copyManager.copyIn(copySql, rddToInputStream(iterator))

        val msg = s"Inserted $rowsCopied records"
        log.warn(msg)
        println(msg)

        connection.close()
      })


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

  private def rddToInputStream(rows: Iterator[Array[Byte]]) = {
    val bytes = rows.flatten
    new InputStream {
      override def read(): Int = if (bytes.hasNext) {
        bytes.next & 0xff // bitwise AND - make the signed byte an unsigned int from 0-255
      } else {
        -1
      }
    }
  }
  /**
    * Adapt to the very specific Safegraph Schema
    */
  private def adaptSpecific(line: String, dateFormat: SimpleDateFormat) = {
    val uuid = new UUID(RANDOM.nextLong(), RANDOM.nextLong())
    val rows = objectMapper.readValues[Array[String]](line)
    val row = rows.nextValue()
    val id = uuid.toString              // NOTE: This is to ensure unique records
    val ts = dateFormat.format(new Date(row(1).toLong))
    val speed = row(2).toDouble
    val dist = row(3).toDouble
    val bearing = row(4).toDouble
    val rtid = row(5).toInt
    val orig = row(6)
    val dest = row(7)
    val secsToDep = row(8).toInt
    val longitude = row(9).toDouble
    val latitude = row(10).toDouble

    val geometryText = "SRID=4326;POINT (" + row(9) + " " + row(10) + ")"
    s"""$id,"$ts",$speed,$dist,$bearing,$rtid,"$orig","$dest",$secsToDep,$longitude,$latitude,"$geometryText"\n""".getBytes
  }
}
