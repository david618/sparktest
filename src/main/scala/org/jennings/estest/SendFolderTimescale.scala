package org.jennings.estest

import java.io.{File, InputStream}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties, UUID}

import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvParser, CsvSchema}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.util.Random

object SendFolderTimescale {
  private val log = LogFactory.getLog(this.getClass)


  def main(args: Array[String]): Unit = {

    TestLogging.setStreamingLogLevels()

    val appName = getClass.getName

    val numargs = args.length

    if (numargs !=  7 && numargs != 8) {
      System.err.println("Usage: SendFolderTimescale foldername [timescaleHost] [recreateTable] [schema] [table] [indexFields] [chunkInterval] { [Spark Master] ")
      System.err.println("        foldername: All files in this folder will be loaded.")
      System.err.println("        timescaleHost: Timescale Server Name or IP")
      System.err.println("        recreateTable: Delete and create table")
      System.err.println("        schema: Schema Name")
      System.err.println("        table: Table Name")
      System.err.println("        indexFields: Index all Fields true/false")
      System.err.println("        chunkInterval: Chunk interval in microseconds")
      System.err.println("        SpkMaster: Spark Master (e.g. local[8] or - to use default)")

      System.exit(1)

    }

    val foldername = args(0)
    val timescaleHost = args(1)
    val recreateTable = args(2)
    val schema = args(3)
    val table = args(4)
    val indexFields = args(5)
    val chunkInterval = args(6)
    val spkMaster = args(7)



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
      )
      TABLESPACE REALTIME
          """.stripMargin
      )



      statement.execute(s"select create_hypertable('$schema.$table', 'ts', chunk_time_interval => $chunkInterval)")

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

      }
      statement.execute(s"create index on $schema.$table using GIST(geometry)")

    }

    log.info("Done initialization, ready to start streaming...")
    println("Done initialization, ready to start streaming...")

    val startTime = System.currentTimeMillis()

    //using COPY command so timestamps have to be formatted as dates
    val simpleFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val sc = new SparkContext(spkMaster, "SendFileCassandra", sConf)

    val folder = new File(foldername)

    val fileArray = folder.list()

    if (fileArray == null) {
      println("No files in %s".format(folder))
    } else {
      val files = fileArray.sortWith(_ < _).iterator

      while (files.hasNext) {
        // For each file in the folder
        val file = files.next
        println(file)
        val filename = foldername + File.separator + file
        val textFile =  sc.textFile(filename).map(line => adaptSpecific(line, simpleFormat))

        // save to timescale
        textFile.foreachPartition( iterator => {
          classOf[org.postgresql.Driver]
          val connection = DriverManager.getConnection(url, properties)
          val copyManager = new CopyManager(connection.asInstanceOf[BaseConnection])
          val copySql = s"""COPY $schema.$table (id,ts,speed,dist,bearing,rtid,orig,dest,secstodep,lon,lat,geohash,sqrhash,pntytrihash,flattrihash,geometry ) FROM STDIN WITH (NULL 'null', FORMAT CSV, DELIMITER ',')"""

          val batchStart = System.currentTimeMillis()
          val rowsCopied = copyManager.copyIn(copySql, rddToInputStream(iterator))

          val msg = s"Inserted $rowsCopied records in ${(System.currentTimeMillis() - batchStart)/1000} s"
          log.info(msg)
          println(msg)

          connection.close()
        })


      }

      val endTime = System.currentTimeMillis()
      log.info(s"Test Duration: ${(endTime - startTime)/1000} s")
      println(s"Test Duration: ${(endTime - startTime)/1000} s")

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
    val geohash = row(11)
    val sqrHash = row(12)
    val pntyTriHash = row(13)
    val flatTriHash = row(14)

    val geometryText = "SRID=4326;POINT (" + row(9) + " " + row(10) + ")"
    s"""$id,"$ts",$speed,$dist,$bearing,$rtid,"$orig","$dest",$secsToDep,$longitude,$latitude,"$geohash","$sqrHash","$pntyTriHash","$flatTriHash","$geometryText"\n""".getBytes
  }


}
