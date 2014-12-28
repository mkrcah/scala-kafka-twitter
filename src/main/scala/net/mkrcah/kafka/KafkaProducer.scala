package net.mkrcah.kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}


object KafkaProducer {

  def send(topic: String, message: String): Unit = {
    val msg = new KeyedMessage[String, String](topic, message)
    kafkaProducer.send(msg)
  }

  private val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, String](config)
  }

}

