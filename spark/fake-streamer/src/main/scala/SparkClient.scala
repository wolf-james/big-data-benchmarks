import org.apache.spark.sql.functions.{from_json, sum, window}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Timestamp

object SparkClient extends App {
  case class Bytes(received: Int,
                   sent: Int,
                   total: Int)

  case class Packets(recv: Int,
                     sent: Int,
                     total: Int)

  case class Remote(ip: String,
                    site: String)

  case class Device(description: String,
                    id: String,
                    ip: String,
                    name: String,
                    os: String,
                    role: String,
                    `type`: String)

  case class Source(timestamp: Timestamp,
                    `type`: String,
                    bytes: Bytes,
                    device: Device,
                    packets: Packets,
                    remote: Remote,
                    tags: Seq[String])

  case class SensorFlowLog(id: String,
                           source: Source,
                           `type`: String)

  // Connect to Apache Spark
  val spark:SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("SparkClientV3")
    .getOrCreate()

  spark.conf.set("spark.sql.streaming.metricsEnabled", "true")

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  // Read from TCP server
  val stream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .option("includeTimestamp", true)
    .load()

  // Parse text stream to JSON
  val sensorFlowLogSchema = Encoders.product[SensorFlowLog].schema
  val jsonStream = stream.select(from_json($"value", sensorFlowLogSchema) as "record")
  val sensorFlowLogStream: Dataset[SensorFlowLog] = jsonStream.select("record.*").as[SensorFlowLog]

  // Create PCR DataFrame
  val pcrDF = sensorFlowLogStream
    .select($"source.timestamp".as("time"),
      $"source.device.ip".as("device_ip"),
      $"source.remote.ip".as("remote_ip"),
      $"source.bytes.recv".as("bytes_recv"),
      $"source.bytes.sent".as("bytes_sent"))
    .withWatermark("time", "5 seconds")

  val groupedPcrDF = pcrDF
    .groupBy(window($"time", "30 minutes"), $"device_ip")
    .agg(
      (sum($"bytes_sent") - sum($"bytes_recv")) / (sum($"bytes_sent") + sum($"bytes_recv")) as "pcr",
    )

  val pcrQuery: Unit = groupedPcrDF.writeStream
    .option("truncate", "false")
    .format("console")
    .outputMode("update")
    .start()
    .awaitTermination()

  // Print PCR DataFrame
  /*
  val pcrQuery: Unit = pcrDF.writeStream
    .option("truncate", "false")
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
  */


  // Print relevant schema
  /*
  stream.printSchema()
  sensorFlowLogStream.printSchema()
  pcrDF.printSchema()
  */

  // Print Root DataFrame
  /*
  val rootQuery: Unit = sensorFlowLogStream.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination(10000)
  */

  // Print PCR DataFrame
  /*
  val pcrQuery: Unit = pcrDF.writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination(10000)
  */

  // Group by Device IP
  /*
  val deviceIpGroupsStream = pcrDF.groupBy(
    window($"time", "30 minutes", "5 minute"),
    $"device_ip"
  ).count()

  val query: Unit = deviceIpGroupsStream.writeStream
    .queryName("deviceIpGroups")
    .outputMode("append")
    .format("console")
    .start()
    .awaitTermination(10000)
  */
}
