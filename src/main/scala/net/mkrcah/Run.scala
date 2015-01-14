package net.mkrcah

import net.mkrcah.Twitter.{Tweet, OnTweetPosted}
import net.mkrcah.kafka.{KafkaConsumer, KafkaProducer}
import twitter4j._

object Run {

  val kafkaTopic = "tweets"
  val kafkaProducer = new KafkaProducer[Array[Byte]]

  val tweetsGpsArea = Array(Array(-126.562500,30.448674), Array(-61.171875,44.087585)) // USA
  
  def main (args: Array[String]) {

    val twitterStream = Twitter.getStream
    twitterStream.addListener(new OnTweetPosted(sendToKafka))

    println("Start producing tweets")
    twitterStream.filter(new FilterQuery().locations(tweetsGpsArea))

    println("Start consuming tweets")
    KafkaConsumer.getMessagesFor(kafkaTopic).foreach(x => printTweet(x.message()))
  }

  def sendToKafka(t:Tweet) {
    val tAvro = TwitterAvroConverter.tweetToAvro(t)
    kafkaProducer.send(kafkaTopic, tAvro)
  }

  def printTweet(arr: Array[Byte]): Unit = {
    val s = TwitterAvroConverter.avroToStr(arr)
    println(s)
  }


}



