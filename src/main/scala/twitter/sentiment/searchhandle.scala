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
 import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j.{LogManager, Level}
import org.apache.commons.logging.LogFactory
import twitter4j._
import scala.collection.JavaConversions._
import com.github.tototoshi.csv._

import java.io.File
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
    //var listcompany=new ListBuffer[String]()
    /*val bufferedSource = Source.fromFile("/home/bkjdev/dev/companylist.csv")
    for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)
        // do whatever you want with the columns here
        println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
        listcompany+=cols(1)
        
    }
    
    bufferedSource.close*/
    def sleep(time: Long) { Thread.sleep(time) }
    val reader = CSVReader.open(new File("/home/bkjdev/dev/companylist.csv"))
    val headerdata= reader.allWithHeaders()
    val listcompany =headerdata.map(x=>x.get("Name"))
    val flatcompany= listcompany.flatMap(flat=> flat) 
    print(flatcompany)
   // print(listcompany.mkString(","))
    val handlelist=new ListBuffer[String]()
   val copanyset=flatcompany.toSet.grouped(14).toList.iterator
    while (copanyset.hasNext ){
      val company=copanyset.next
       while(company.iterator.hasNext){
        val companyVal=company.iterator.next
      
      if(Character.isDigit(companyVal.charAt(0))){
         println(company)
       }else{
        val results =twitter.searchUsers(companyVal,1)
        if(results.size()>0)
       handlelist+=results.head.getScreenName
       sleep(900000)
        //
       }
       }
      } 
  //  val result s =twitter.searchUsers(company,1)
  //  handlelist+=results.head.getScreenName
  //  }   
    //val listTweet =result.getTweets()
    for (status <-handlelist) {
      println(status)
      //println(status.getScreenName +"--"+ status.getName +"--"+ status.getDescription)
    }         
    }
} 
}