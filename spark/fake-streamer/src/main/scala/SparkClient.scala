// Spark client
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkClient extends App {
  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("SparkClient")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  // Create a socket stream on target ip:port and count the
  // words in input stream of \n delimited text (e.g. generated by 'nc')
  // Note that no duplication in storage level only for running locally.
  // Replication necessary in distributed scenario for fault tolerance.
  val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  val wordCounts = lines.map(x => (x, 1)).reduceByKey(_ + _)

  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}
