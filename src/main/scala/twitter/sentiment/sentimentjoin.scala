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
import org.apache.spark.sql.SparkSession

object sentimentjoin {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }
    val appName = "TwitterData"
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate

   // val conf = new SparkConf()
    //conf.setAppName(appName).setMaster("local[2]")
   // val ssc = new StreamingContext(conf, Seconds(5))
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

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
   val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Read in the word-sentiment list and create a static RDD from it
    val wordSentimentFilePath = "C://dev/AFINN-111.txt"
    val wordSentiments = ssc.sparkContext.textFile(wordSentimentFilePath).map { line =>
      val Array(word, happinessValue) = line.split("\t")
      (word, happinessValue.toInt)
    }.cache()
    val dfWithoutSchema = spark.createDataFrame(wordSentiments)

    dfWithoutSchema.show()
    // Determine the hash tags with the highest sentiment values by joining the streaming RDD
    // with the static RDD inside the transform() method and then multiplying
    // the frequency of the hash tag by its sentiment value
    val happiest60 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(60))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))
    

    val happiest10 = hashTags.map(hashTag => (hashTag.tail, 1))
      .reduceByKeyAndWindow(_ + _, Seconds(10))
      .transform{topicCount => wordSentiments.join(topicCount)}
      .map{case (topic, tuple) => (topic, tuple._1 * tuple._2)}
      .map{case (topic, happinessValue) => (happinessValue, topic)}
      .transform(_.sortByKey(false))

    // Print hash tags with the most positive sentiment values
    happiest60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nHappiest topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("%s (%s happiness)".format(tag, happiness))}
    })

    happiest10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nHappiest topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (happiness, tag) => println("%s (%s happiness)".format(tag, happiness))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}