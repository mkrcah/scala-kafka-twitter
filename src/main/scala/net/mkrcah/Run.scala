package net.mkrcah

import com.typesafe.config.ConfigFactory
import net.mkrcah.avro.TweetsAvro
import net.mkrcah.kafka.{KafkaConsumer, KafkaProducer}
import twitter4j._
import twitter4j.conf.{Configuration, ConfigurationBuilder}

object Run  {

  type Tweet = twitter4j.Status

  val conf = ConfigFactory.load()

  val kafkaTopic = "tweets"
  val kafkaProducer = new KafkaProducer[Array[Byte]]

  val tweetsGpsArea = Array(Array(-126.562500,30.448674), Array(-61.171875,44.087585)) // USA
  
  def main (args: Array[String]) {

    val twitterStream = new TwitterStreamFactory(getTwitterConf).getInstance()
    twitterStream.addListener(new OnTweetPostedListener(sendToKafka))

    twitterStream.filter(new FilterQuery().locations(tweetsGpsArea))

    println("Start consuming tweets")
    KafkaConsumer.getMessagesFor(kafkaTopic).foreach(x => printTweet(x.message()))
  }

  def sendToKafka(t:Tweet) {
    val tAvro = TweetsAvro.tweetToAvro(t)
    kafkaProducer.send(kafkaTopic, tAvro)
  }

  def printTweet(s: String): Unit = {
    println(s)
  }

  def getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(conf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(conf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(conf.getString("twitter.accessTokenSecret"))
      .build()
    twitterConf
  }

}


class OnTweetPostedListener(cb: Status => Unit) extends StatusListener {

  override def onStatus(status: Status): Unit = cb(status)
  override def onException(ex: Exception): Unit = throw ex

  // no-op for the following events
  override def onStallWarning(warning: StallWarning): Unit = {}
  override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
  override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
  override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
}


