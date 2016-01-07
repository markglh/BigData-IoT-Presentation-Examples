
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

/**
  * This parses hashtags out of tweets and counts them over 60 second windows
  * To run this you will need to create twitter.properties in the root of the application, see TwitterHelper.scala for more info.
  */
object TwitterStreamCount {
  def main(args: Array[String]) {
    //Create you twitter tokens here: https://apps.twitter.com/
    TwitterHelper.configureTwitterCredentials()

    //Create a conf, running on two local cores
    val conf = new SparkConf()
      .setAppName("Twitter HashTag Counter")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("./checkpoint") // We need somewhere to store the state

    //Receive all tweets unfiltered
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)

    //We're not doing anything fancy with re-tweets etc
    //Split tweets into words
    val words = stream.flatMap(tweet => tweet.getText.toLowerCase.split(" "))

    //Pull out the hashtags and map them to a count
    val hashtags = words.filter(word => word.startsWith("#"))

    /*val tagCountsOverWindow = hashtags
      .map((_, 1))
      //60 second window
      .reduceByKeyAndWindow(_ + _, Seconds(60))*/

    //This is exactly the same as what's commented out above:
    val tagCountsOverWindow = hashtags
      //60 second window
      .countByValueAndWindow(Seconds(60), hashtags.slideDuration)

    val sortedTagCounts = tagCountsOverWindow
      //Swap the key and value to make sorting easier
      //This requires shuffling data as we're swapping the keys around
      .map { case (hashtag, count) => (count, hashtag) }
      .transform(_.sortByKey(false))

    //Print the Top 10
    sortedTagCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}