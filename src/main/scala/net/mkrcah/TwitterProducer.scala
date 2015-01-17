package net.mkrcah

import net.mkrcah.Twitter.{OnTweetPosted, Tweet}
import net.mkrcah.kafka.KafkaProducer
import twitter4j._

object TwitterProducer {

  val KafkaTopic = "tweets"

  private val kafkaProducer = new KafkaProducer[Array[Byte]]
  private val tweetsGpsArea = Array(Array(-126.562500,30.448674), Array(-61.171875,44.087585)) // USA
  
  def main (args: Array[String]) {

    val twitterStream = Twitter.getStream
    twitterStream.addListener(new OnTweetPosted(sendToKafka))

    println("Start producing tweets")
    twitterStream.filter(new FilterQuery().locations(tweetsGpsArea))
  }

  def sendToKafka(t:Tweet) {
    println(t.getText)
    val encTweet = TwitterAvroCodec.encode(t)
    kafkaProducer.send(KafkaTopic, encTweet)
  }


}



