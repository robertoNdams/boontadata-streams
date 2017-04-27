package StructuredStream

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.log4j.Level
import collection.JavaConversions._ 
import com.datastax.driver.core.Session
import org.apache.spark.sql.functions._ 
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.ForeachWriter
import com.datastax.spark.connector.cql.CassandraConnector
import java.text.SimpleDateFormat


object Main {

  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    Logger.getLogger("com.datastax").setLevel(Level.WARN)
    Logger.getLogger("kafka").setLevel(Level.WARN)

    logger.setLevel(Level.INFO)

    val sparkJob = new SparkJob()
    try {
      sparkJob.runJob()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
    }
  }
}


object Commons {
def getTimeStamp(timeStr: String): Timestamp = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(timeStr.toLong)
    new Timestamp(dateFormat.parse(date).getTime)
  } 
}
object Statements extends Serializable {
  def cql(device_id: String, category:String, window_time: Timestamp, m1_sum_downstream: Double, m2_sum_downstream: Double): String = s"""
       insert into boontadata.agg_events(device_id, category, window_time,m1_sum_downstream,m2_sum_downstream)
       values('$device_id','$category', '$window_time', '$m1_sum_downstream','$m2_sum_downstream')"""
}
case class UserEvent(device_id: String, category:String, window_time:Timestamp, m1_sum_downstream: Double, m2_sum_downstream: Double)
      extends Serializable
      
      
class SparkJob extends Serializable{

val spark = SparkSession.builder
    .appName("boontadata-DirectStreamKafkaAggregateEvents")
    .master("spark://sstreamm1:6066")
    .config("spark.cassandra.connection.host", "cassandra1,cassandra2,cassandra3")
    .getOrCreate()

    
val connector = CassandraConnector.apply(spark.sparkContext.getConf)

 private def processRow(value: UserEvent) = {
    connector.withSessionDo { session =>
      session.execute(Statements.cql(value.device_id, value.category, value.window_time, value.m1_sum_downstream, value.m2_sum_downstream))
    }
  }
  
  def runJob(){
  
 import spark.implicits._
val kafka = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "ks1:9092,ks2:9092,ks3:9092")   // comma separated list of broker:host
  .option("subscribe", "sampletopic")    // comma separated list of topics
  .option("startingOffsets", "latest") // read data from the end of the stream
  .load()
  
val kf = kafka.selectExpr("CAST(value AS STRING)", "timestamp").as[(String, String)]
 val cols = List("messageId","device_id","timestamp","category","measure1","measure2")  
  val df =
      kf.map { line =>
        val columns = line._1.split("\\|") 
        (columns(0), columns(1), Commons.getTimeStamp(columns(2)), columns(3), columns(4), columns(5))
      }.toDF(cols: _*)
      
val agg = df.withWatermark("timestamp", "5 seconds").groupBy($"device_id",$"category", $"timestamp".as("window_time"),$"measure1",$"measure2")
            .agg(sum($"measure1").as("m1_sum_downstream"),sum($"measure2").as("m2_sum_downstream"))
val directAgg = agg.select($"device_id", $"category", $"window_time", $"m1_sum_downstream", $"m2_sum_downstream").as[UserEvent]

directAgg.printSchema()
  
val writer = new ForeachWriter[UserEvent] {
        override def open(partitionId: Long, version: Long)= true 
        override def process(value: UserEvent) = {
          processRow(value)
        }
        override def close(errorOrNull: Throwable): Unit = { }
      } 
      
val direct = directAgg.writeStream  
   .queryName("AggregationStructuredStream")
   .foreach(writer)
   .start()
 
/*
val query = directAgg.writeStream
  .outputMode("complete")
  .option("truncate", "false")
  .format("console")
  .start()
 */
 
direct.awaitTermination() 
spark.stop() 
}
}


