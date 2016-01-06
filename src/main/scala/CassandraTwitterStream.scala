
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils


case class HashTagCount(hashtag: String, count: Long)

/**
  * This parses hashtags out of tweets and counts them over 60 second windows
  * To run this you will need to create twitter.properties in the root of the application, see TwitterHelper.scala for more info.
  */
object CassandraTwitterStream {
  def main(args: Array[String]) {


    //Create you twitter tokens here: https://apps.twitter.com/
    TwitterHelper.configureTwitterCredentials()

    //Create a conf, running on two local cores
    val conf = new SparkConf()
      .setAppName("Twitter HashTag Counter")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "127.0.0.1")

    val ssc = new StreamingContext(conf, Seconds(1))

    //Create the Cassandra schema and tables
    initCassandra(conf)

    //Receive all tweets unfiltered
    val stream = TwitterUtils.createStream(ssc, None)

    //Split tweets into words
    val words = stream.flatMap(tweet => tweet.getText.toLowerCase.split(" "))

    //Pull out the hashtags and map them to a count
    val hashtags = words.filter(word => word.startsWith("#"))


    /*hashtags
      .reduceByKey(_ + _)
      .map(tags => (tags._1, tags._2, new Date()))
      .saveToCassandra("spark", "wordcount", SomeColumns("hashtag", "count"))*/

    //alternatively we could use case classes:
    hashtags
      .countByValue()
      .map(rdd => HashTagCount(rdd._1, rdd._2))
      .saveToCassandra("spark", "wordcount")

    ssc.start()
    ssc.awaitTermination()
  }

  def initCassandra(conf: SparkConf) = {
    //Create the keyspace and table in Cassandra
    CassandraConnector(conf).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS spark WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 }")
      session.execute(s"CREATE TABLE IF NOT EXISTS spark.wordcount (hashtag TEXT, count COUNTER, PRIMARY KEY (hashtag))")
      session.execute(s"TRUNCATE spark.wordcount")
    }


  }
}