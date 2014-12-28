package net.mkrcah

import com.typesafe.config.ConfigFactory
import net.mkrcah.kafka.{KafkaConsumer, KafkaProducer}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object TweetsToKafkaExample  {

  type Tweet = twitter4j.Status

  val conf = ConfigFactory.load()

  val kafkaTopic = "tweets"

  def main (args: Array[String]) {

    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(conf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(conf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(conf.getString("twitter.accessTokenSecret"))
      .build()

    val twitterStream = new TwitterStreamFactory(twitterConf).getInstance()

    def sendToKafka(t:Tweet): Unit = KafkaProducer.send(kafkaTopic, asString(t))
    twitterStream.addListener(new TweetPostedListener(sendToKafka))

    // start listening to tweets from the US region and storing them to Kafka
    val usGPSArea = Array(Array(-126.562500,30.448674), Array(-61.171875,44.087585))
    twitterStream.filter(new FilterQuery().locations(usGPSArea))

    // read messages from Kafka
    println("Start consuming tweets")
    val tweetStream = KafkaConsumer.getMessagesFor(kafkaTopic).map(_.message())
    tweetStream.foreach(println)
  }

  def asString(s:Tweet):String = {
    val name = s.getUser.getScreenName.padTo(15, " ").mkString("")
    val text = s.getText.replaceAll("\n", "")
    s"$name => $text"
  }

}


class TweetPostedListener(onTweetPosted: Status => Unit) extends StatusListener {

  override def onStatus(status: Status): Unit = onTweetPosted(status)
  override def onException(ex: Exception): Unit = throw ex

  // no-op for the following events
  override def onStallWarning(warning: StallWarning): Unit = {}
  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
  override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
  override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
}


