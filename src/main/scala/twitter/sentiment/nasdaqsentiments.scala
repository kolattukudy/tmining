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
import org.apache.spark.sql.functions._
object nasdaqsentiments {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }
     val appName = "Nasdaq sentiments"
    val spark: SparkSession = SparkSession.builder.master("local[2]").getOrCreate
    import org.apache.spark.sql.types._
    import spark.implicits._ 
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val schema = StructType(Array(
    StructField("Symbol", StringType, true),
    StructField("Name", StringType, true),
    StructField("LastSale", StringType, true),
    StructField("MarketCap", StringType, true),
    StructField("IPOyear", StringType, true),
    StructField("Sector", StringType, true),

    StructField("industry", StringType, true)

  ))
    val staticdf = spark.read.option("header", "true").schema(schema).csv("C:/data/companylist.csv").drop("LastSale","IPOyear")
    
    staticdf.show(false)
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
    val hashTags = stream.filter(_.getLang()=="en").flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val hashTags2 = stream.filter(_.getLang()=="en").filter({
      {t => 
       val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
       !tags.isEmpty 
    }
    })

    val topCounts60 = hashTags2.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))

    val topCounts10 = hashTags2.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
    
    val sqlContex = spark.sqlContext
   
    val data1 = topCounts60.map { status =>
    val sentiment = SentimentAnalysisUtils.detectSentiment(status._2.getText)
    val tags = status._2.getHashtagEntities.map(_.getText.toLowerCase)
    // (status._2.getText, sentiment.toString, status._2.getUser.getName,status._2.getCreatedAt.getTime,status._2.getSource,status._2.getPlace.getCountry)
    (tags,status._2.getText, sentiment.toString)
    }
    data1.cache().foreachRDD(rdd => {
      val df = spark.createDataFrame(rdd)
      val newdf=df.withColumn("hashtags",explode(col("_1")))
      val list=staticdf.select("Symbol").map(r => r.getString(0)).collect.toList 
      val filterdf=newdf.filter($"hashtags".isin(list:_*)).withColumnRenamed("_3", "sentiment").withColumnRenamed("_2", "text").withColumnRenamed("_1", "tagsarray")

      filterdf.createOrReplaceTempView("sentiments")
      filterdf.show(false)
      //sqlContex.sql("select * from sentiments limit 20").show(false)
     })       
    ssc.start()
    ssc.awaitTermination()
  }

}