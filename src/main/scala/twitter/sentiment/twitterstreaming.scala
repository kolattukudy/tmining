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
object twitterstreaming {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }
    val appName = "TwitterData"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val sc = ssc.sparkContext
    import org.apache.log4j.{LogManager, Level}
    import org.apache.commons.logging.LogFactory
    LogManager.getRootLogger().setLevel(Level.ERROR)
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    val cb = new ConfigurationBuilder
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      
    val auth = new OAuthAuthorization(cb.build)
    val stream = TwitterUtils.createStream(ssc, Some(auth), filters)
    //tweets .saveAsTextFiles("tweets", "json")
    //val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.filter(_.getLang()=="en").filter {t => 
      
    val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      //print(tags.deep.mkString("\n"))
      //tags.contains("#bigdata") && tags.contains("#food")&& tags.contains("#love")&& tags.contains("#life")
      tags.contains("#bigdata") || tags.contains("#food")|| tags.contains("#trump")|| tags.contains("#FridayFeeling") || tags.contains("#MAGABomber")

    }
    val data = tweets.map { status =>
    val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
    val tags = status.getHashtagEntities.map(_.getText.toLowerCase)
    println(status.getText )
   // println(sentiment.toString())
   
    (status.getText, sentiment.toString, tags.mkString("\n"))
}
   
    //tweets.print()
    data.print()
    ssc.start()
    ssc.awaitTermination()
  }
}