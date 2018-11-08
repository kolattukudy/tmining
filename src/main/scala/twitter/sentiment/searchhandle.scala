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
import twitter4j._

object searchhandle {
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
   //https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup.html
    //http://www.tothenew.com/blog/search-twitter-users-using-twitter4j/
    // val auth = new OAuthAuthorization(cb.build)
    val twitter = new TwitterFactory(cb.build()).getInstance();
    val cursor = -1.longValue()
    import scala.io.Source
    import scala.collection.mutable.ListBuffer
    var listcompany=new ListBuffer[String]()
    val bufferedSource = Source.fromFile("/home/bkjdev/dev/companylist.csv")
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        // do whatever you want with the columns here
        println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
        listcompany+=cols(0)
        
    }
    
    bufferedSource.close
    print(listcompany.mkString(","))
    val query = new Query("Harley Davidson")
    val searchString ="Harley Davidson"
    val result =twitter.searchUsers(searchString,1)
    import scala.collection.JavaConversions._
    
    //val listTweet =result.getTweets()
    for (status <-result) {
      println(status.getScreenName +"--"+ status.getName +"--"+ status.getDescription)
    }
    
     
    }
   }
   
}