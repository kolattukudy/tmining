package twitter.sentiment

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.twitter._
import twitter.sentiment.utils._
import twitter4j.FilterQuery
import twitter4j.TwitterFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{LogManager, Level}
import org.apache.commons.logging.LogFactory

object findfollowers {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }else{
    LogManager.getRootLogger().setLevel(Level.ERROR)
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
       .setUserStreamRepliesAllEnabled(true)

   // val auth = new OAuthAuthorization(cb.build)
    val twitter = new TwitterFactory(cb.build()).getInstance();
    val cursor = -1.longValue()
    println(twitter.users().getAccountSettings.getScreenName)
    val ids = twitter.getFollowersIDs("bkkudy", cursor);
    println (ids.getIDs.mkString(","))
    
    val friendId=twitter.friendsFollowers().getFollowersIDs("bkkudy",cursor)
    println(friendId.getIDs.mkString(","))
    
    val followerId=twitter.getFriendsIDs("bkkudy",cursor)
    println(followerId.getIDs.mkString(","))
    
    for (id <- followerId.getIDs){
      
      println(twitter.showUser(id).getName + "--"+ id)
    }
     }
    }
  }

  