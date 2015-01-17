package net.mkrcah

import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object SparkConsumer extends App{

  val conf = new SparkConf().setAppName("kafka-twitter-spark-example").setMaster("local[*]")
  val sc = new StreamingContext(conf, Seconds(5))

  val avroTweets = {
    val topics = Map(TwitterProducer.KafkaTopic -> 1)
    val kafkaParams = Map("zookeeper.connect" -> "localhost:2181", "group.id" -> "1")
    KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
  }

  val tweets = avroTweets.map(x => TwitterAvroCodec.decode(x._2))

  val wordCounts = tweets.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
  val countsSorted = wordCounts.transform(_.sortBy(_._2, ascending = false))

  countsSorted.print()

  sc.start()
  sc.awaitTermination()
}
