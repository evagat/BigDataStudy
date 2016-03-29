package org.study.group.tweetsTreatment

/**
  * Created by egarcia on 21/02/16.
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.study.group.kafka.KafkaProducer


object TweetsReader  {
  def main(args: Array[String]) {

    //Authentication
    val consumerKey = "<consumer_key>"
    val consumerSecret = "<consumer_secret>"
    val accessToken = "<accessToken>"
    val accessTokenSecret = "<accessTokenSecret>"

    // System properties so that Twitter4j library used by twitter stream
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    //Don't print info and warning messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Create a local StreamingContext with two working thread and batch interval of 2 second.
    // The master requires 2 cores to prevent from a starvation scena
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwitterHashTags")
    val ssc = new StreamingContext(conf, Seconds(2))

    //Filters
    val filters = Array("Barça","Barsa","Real Madrid")

    //Stream
    val stream = TwitterUtils.createStream(ssc,None,filters)

    //Tweets extractions
    val tweet  = stream.map(rdd =>{
      //Message
      val twt = (rdd.getUser.getName, rdd.getText)
      //Send to Kafka
      KafkaProducer.produceIntoKafka(twt.toString,"tweets","localhost:9092")
    })

    tweet.print

    ssc.start()
    ssc.awaitTermination()
  }
}
