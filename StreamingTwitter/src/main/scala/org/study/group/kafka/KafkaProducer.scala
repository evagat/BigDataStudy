package org.study.group.kafka

/**
  * Created by egarcia on 17/03/16.
  */

import java.util.Properties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

object KafkaProducer{
  def produceIntoKafka(msg: Object,nameTopic:String,brokers:String) {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, Object](config)
    val data = new KeyedMessage[String, Object](nameTopic, msg);
    producer.send(data);

    producer.close();
  }
}
