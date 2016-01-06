import java.io.FileInputStream
import java.util.Properties

/**
  * Helper function idea from here:
  * https://github.com/amplab/training/blob/88be6dfd52b60441844d10963b77365b52955937/streaming/scala/TutorialHelper.scala
  */
object TwitterHelper {

  /**
    * Loads twitter config from twitter.txt (from root dir) in the format:
consumerKey = xxx
consumerSecret = xxx
accessToken = 	xxx
accessTokenSecret = xxx

Go here to create your tokens:
    https://apps.twitter.com/
    */
  def configureTwitterCredentials() {
    val prop = new Properties()
    prop.load(new FileInputStream("twitter.properties"))

    //This will be brittle, don't use in prod
    System.setProperty("twitter4j.oauth.consumerKey", prop.getProperty("consumerKey"))
    System.setProperty("twitter4j.oauth.consumerSecret", prop.getProperty("consumerSecret"))
    System.setProperty("twitter4j.oauth.accessToken", prop.getProperty("accessToken"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", prop.getProperty("accessTokenSecret"))
  }

}
