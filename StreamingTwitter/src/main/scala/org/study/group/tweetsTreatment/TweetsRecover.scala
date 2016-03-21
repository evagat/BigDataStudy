package org.study.group.tweetsTreatment

/**
  * Created by egarcia on 17/03/16.
  */

import com.datastax.driver.core.{Cluster, Session}
import org.study.group.cassandra.CassandraOperations
import org.study.group.kafka.KafkaConsumer

object TweetsRecover {
  def main(args: Array[String]) {
    //Connection
    val cluster: Cluster = CassandraOperations.connectCluster("127.0.0.1")
    val session: Session = CassandraOperations.connectSession(cluster)

    //KeySpace and Table Names
    val nameKeyspace = "keyspaceTweets"
    val nameTable = "tableTweets"

    //Keyspace creation
    CassandraOperations.createKeySpace(nameKeyspace,cluster,session)

    //Table creation
    CassandraOperations.createTable(nameKeyspace,nameTable, cluster,session, Map("user" -> "text",
                                                                                 "tweet" -> "text"))
    //Disconnection
    CassandraOperations.disconnect(cluster,session)

    //Extract tweets from Kafka
    val msg = KafkaConsumer.consumeFromKafka("tweets","twitterGroup","localhost:2181",true)

  }

  def sendMessageToCassandra(thread:Int, message:String): Unit ={
        //Connection
        val cluster: Cluster = CassandraOperations.connectCluster("127.0.0.1")
        val session: Session = CassandraOperations.connectSession(cluster)

        //KeySpace and Table Names
        val nameKeyspace = "keyspaceTweets"
        val nameTable = "tableTweets"

       //Parser
        val msg = message.split(",")
        val user = msg(0).replace("("," ").replace(")"," ")
        val tweet = msg(1).replace("("," ").replace(")"," ")

        //Save to Cassandra
        CassandraOperations.insertData(nameKeyspace, nameTable, cluster, session, Map("id" -> thread.toString,
                                                                                      "user" -> user ,
                                                                                      "tweet" -> tweet) )

        //Disconnection
        CassandraOperations.disconnect(cluster,session)

  }
}
