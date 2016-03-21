package org.study.group

import org.scalatest.ShouldMatchers
import org.study.group.kafka.KafkaConsumer
import org.study.group.kafka.KafkaProducer
import org.scalatest.FlatSpec

class testKafka extends FlatSpec with ShouldMatchers {
    "testkafka" should "pass" in {
        KafkaProducer.produceIntoKafka("janderclander", "test2", "localhost:9092")
        KafkaConsumer.consumeFromKafka("test2", "twitterGroup", "localhost:2181", true)
    }
}
