package net.mkrcah.kafka

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}

object KafkaConsumer {

   def getMessagesFor(topic: String): Stream[MessageAndMetadata[String, Array[Byte]]] = {
     val filterByTopic = new Whitelist(topic)
     val streams = consumer.createMessageStreamsByFilter(filterByTopic, 1, new StringDecoder(), new DefaultDecoder())
     streams(0).toStream
   }

   private val props = {
     val props = new Properties()
     props.put("group.id", "group1")
     props.put("zookeeper.connect", "")
     props.put("auto.offset.reset", "largest")
     props
   }

   private val consumer = {
     val config = new ConsumerConfig(props)
     Consumer.create(config)
   }

 }
