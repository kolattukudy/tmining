
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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object followusersentiments {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      System.exit(1)
    }
     val appName = "Nasdaq sentiments"
    val spark: SparkSession = SparkSession.builder.master("local[2]").config("spark.sql.warehouse.dir", "/tmp").getOrCreate
    
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
    val staticdf = spark.read.option("header", "true").schema(schema).csv("/home/bkjdev/dev/companylist.csv").drop("LastSale","IPOyear")
    
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
 
   // val hashTags = stream.filter(_.getLang()=="en").filter(_.getUser().getId==25073877).flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    //filter by user id
    val hashTags2 = stream.filter(_.getLang()=="en").filter(_.getUser().getId==25073877).filter({
      {t => 
       val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
       !tags.isEmpty 
    }
    })
   //get the top hashtag by 60 seconds
    val topCounts60 = hashTags2.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                     .map{case (topic, count) => (count, topic)}
                     .transform(_.sortByKey(false))
    val sqlContex = spark.sqlContext
   //create  tuple with tag, text and sentiment 
    val data1 = topCounts60.map { status =>
    val sentiment = SentimentAnalysisUtils.detectSentiment(status._2.getText)
    val tags = status._2.getHashtagEntities.map(_.getText.toLowerCase)
    // (status._2.getText, sentiment.toString, status._2.getUser.getName,status._2.getCreatedAt.getTime,status._2.getSource,status._2.getPlace.getCountry)
    val mentionString= status._2.getUserMentionEntities.map(_.getText.toLowerCase())
    
    (tags,status._2.getText, sentiment.toString,mentionString)
    }
    data1.cache().foreachRDD(rdd => {
      //create rdd with tuple
      val df = spark.createDataFrame(rdd)
      //explode the hastags
      val newdf=df.withColumn("mentions",explode(col("_4")))
      //create list from the company dataframe
      val list=staticdf.select("Symbol").map(r => r.getString(0)).collect.toList 
      //check if the user is mentioning about the symbol or company 
      //by filtering the incoming streaming rdd with the list of user company tags
      val filterdf=newdf.filter($"mentions".isin(list:_*)).withColumnRenamed("_3", "sentiment").withColumnRenamed("_2", "text").withColumnRenamed("_1", "tagsarray")
      
      //now store the content in hdfs
       if(!filterdf.rdd.isEmpty()){
        filterdf.write.mode("append").parquet("/data/bjose") 
       }
      filterdf.createOrReplaceTempView("sentiments")
      filterdf.show(false)
      //sqlContex.sql("select * from sentiments limit 20").show(false)
     })       
    ssc.start()
    ssc.awaitTermination()
  }

}