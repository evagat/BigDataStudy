package org.study.group.kafka

/**
  * Created by egarcia on 17/03/16.
  */

import java.util.Properties
import com.datastax.driver.core.{Cluster, Session}
import kafka.consumer.{Consumer, ConsumerConfig, Whitelist}
import kafka.serializer._
import org.study.group.tweetsTreatment.TweetsRecover
import scala.collection.JavaConversions._

object KafkaConsumer {
  def consumeFromKafka(topic:String, groupId: String,zookeeper: String,  readFromStartOfStream: Boolean = true): Unit = {
    val props = new Properties()
    props.put("group.id", groupId)
    props.put("zookeeper.connect", zookeeper)
    props.put("auto.offset.reset", if(readFromStartOfStream) "smallest" else "largest")

    val config = new ConsumerConfig(props)
    val connector = Consumer.create(config)
    val filterSpec = new Whitelist(topic)

    val stream = connector.createMessageStreamsByFilter(filterSpec, 1, new StringDecoder(), new StringDecoder()).get(0)

    var thread = 0
    val consumer = TweetsRecover

    for(messageAndTopic <- stream) {
      try {
        thread+=1
        System.out.println(messageAndTopic.message)
        consumer.sendMessageToCassandra(thread,messageAndTopic.message)

      } catch {
        case e: Throwable =>
          if (true) {
            System.out.println("Error processing message, skipping this message: "+ e)
          }
          else {
            throw e
          }
      }
    }



  }
}
