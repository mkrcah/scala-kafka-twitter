package net.mkrcah

import kafka.serializer.{DefaultDecoder, StringDecoder}
import com.twitter.bijection.avro.SpecificAvroCodecs
import net.mkrcah.avro.Tweet
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import scala.collection.JavaConversions._

object KafkaConsumerApp extends App{

  val conf = new SparkConf().setAppName("kafka-twitter-spark-example").setMaster("local[*]")
  val sc = new StreamingContext(conf, Seconds(5))

  val encTweets = {
    val topics = Map(KafkaProducerApp.KafkaTopic -> 1)
    val kafkaParams = Map("zookeeper.connect" -> "localhost:2181", "group.id" -> "1")
    KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
  }

  val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)

  val wordCounts = tweets.flatMap(_.getText.split(" ")).map((_,1)).reduceByKey(_ + _)
  val countsSorted = wordCounts.transform(_.sortBy(_._2, ascending = false))

  countsSorted.print()

  sc.start()
  sc.awaitTermination()
}
