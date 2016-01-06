import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._

import scala.collection.mutable


object StreamWordCount {
  def main(args: Array[String]): Unit = {

    //Create a conf, running on two local cores
    val conf = new SparkConf()
      .setAppName("Streaming Word Count")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))
    val context = ssc.sparkContext

    //We're using a mutable queue with 3 items
    val rddQueue: mutable.Queue[RDD[String]] = mutable.Queue()
    rddQueue += context.textFile("words.txt")
    rddQueue += context.textFile("words2.txt")
    rddQueue += context.textFile("words3.txt")

    //the queue acts as the source receiver, removing one item each time
    val streamBatches = ssc.queueStream(rddQueue, oneAtATime = true)
    val words = streamBatches.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    //print the first 10 elements of each batch
    wordCounts.print()

    //Start the stream
    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }
}
