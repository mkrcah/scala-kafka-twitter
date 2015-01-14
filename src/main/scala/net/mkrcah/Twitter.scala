package net.mkrcah

import com.typesafe.config.ConfigFactory
import twitter4j._
import twitter4j.conf.{ConfigurationBuilder, Configuration}

object Twitter {

  type Tweet = twitter4j.Status

  private val conf = ConfigFactory.load()

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey(conf.getString("twitter.consumerKey"))
      .setOAuthConsumerSecret(conf.getString("twitter.consumerSecret"))
      .setOAuthAccessToken(conf.getString("twitter.accessToken"))
      .setOAuthAccessTokenSecret(conf.getString("twitter.accessTokenSecret"))
      .build()
    twitterConf
  }


  def getStream = new TwitterStreamFactory(getTwitterConf).getInstance()

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = cb(status)
    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }


}
