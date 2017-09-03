package com.stdatalabs.SparkES

import com.stdatalabs.SparkES.SentimentUtils._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

import scala.util.Try

/**
 * Twitter Sentiment analysis using Stanford core-nlp library
 * and storing results in elastic search
 * 
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <topic-name> <keyword_1> ... <keyword_n>
 * <comsumerKey>	- Twitter consumer key 
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>	- Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * <topic-name>		- The kafka topic to subscribe to
 * <keyword_1>		- The keyword to filter tweets
 * <keyword_n>		- Any number of keywords to filter tweets
 * 
 * 
 * @author Sachin Thirumala
 */

object TwitterSentimentAnalysis {

   def main(args: Array[String]) {

     if (args.length < 4) {
       System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
         "<access token> <access token secret> [<filters>]")
       System.exit(1)
     }

     val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
     val filters = args.takeRight(args.length - 4)

     // set twitter oAuth keys
     System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
     System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
     System.setProperty("twitter4j.oauth.accessToken", accessToken)
     System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

     val conf = new SparkConf().setAppName("TwitterSentimentAnalysis")
     
     // Create a DStream for every 5 seconds
     val ssc = new StreamingContext(conf, Seconds(5))

     // Get json object from twitter stream
     val tweets = TwitterUtils.createStream(ssc, None, filters)

     tweets.print()

     /* Extract and transform required columns from json object and also generate sentiment score for each tweet.
      * RDD can be saved into elasticsearch as long as the content can be translated to a document.
      * So each RDD should be transformed to a Map before storing in elasticsearch index twitter_082717/tweet.
      */
     tweets.foreachRDD{(rdd, time) =>
       rdd.map(t => {
         Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt.getTime.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "text" -> t.getText,
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           "language" -> t.getLang.toString(),
           "sentiment" -> detectSentiment(t.getText).toString
         )
       }).saveToEs("twitter_020717/tweet")
     }
     

     ssc.start()
     ssc.awaitTermination()

   }
 }