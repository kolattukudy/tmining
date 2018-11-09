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
import scala.concurrent.{future, blocking, Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

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
      val twitter = new TwitterFactory(cb.build()).getInstance();
    val cursor = -1.longValue()
    import scala.io.Source
    import scala.collection.mutable.ListBuffer
   val file = "whatever.txt"
   import java.io._

  val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    def sleep(time: Long) { Thread.sleep(time) }
    val reader = CSVReader.open(new File("/users/bjdev/companylist.csv"))
    val headerdata= reader.allWithHeaders()
    val listcompany =headerdata.map(x=>x.get("Name"))
    val flatcompany= listcompany.flatMap(flat=> flat) 
    print(flatcompany)
   // print(listcompany.mkString(","))
    val handlelist=new ListBuffer[String]()
    val myFuts: List[Future[String]] = flatcompany.map {myid => 
    val myfut = future { val results=twitter.searchUsers(myid,1)
    handlelist+=results.head.getScreenName 
    Thread.sleep(90000)
    results.head.getScreenName 
    }
  val result = for (myval <- myfut) yield {
    val res = s"SUCCESS $myid: $myval"
    val sucessval=s"$myid,$myval"
    println(res)
    writer.write(sucessval)
  
    // for (status <-handlelist) {println(status)} 
    res
  }
  result.recover{
    case ex => 
     val failureval=s"$myid,"
     writer.write(failureval)
     
      println (s"failed with error:$myid : $ex")
      "FAILED"          
  }

}
  writer.close()
val futset: Future[List[String]] = Future.sequence(myFuts)
println (Await.result(futset, 1000 seconds))
    }
   }
}
