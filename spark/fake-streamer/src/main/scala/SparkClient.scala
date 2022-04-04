import org.apache.spark.sql.functions.from_json
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
    .appName("SparkClient")
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  // Read from TCP server
  val stream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
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
}
