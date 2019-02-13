import com.datastax.driver.core.ConsistencyLevel
import org.apache.spark.SparkConf

val c1 = ConsistencyLevel.ANY.toString
val c2 = ConsistencyLevel.ONE.toString

val sConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "foo")
    .set("spark.cassandra.output.consistency.level", c1)
    .setAppName("SendKafkaTopicCassandraPlanesHashGlobalObjectIds")


val conf = sConf.getAll
