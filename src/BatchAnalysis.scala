package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Buffer

object BatchAnalysis {
	val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
 	val sc = new SparkContext(conf)
	sc.setLogLevel("WARN")

	// For each tweet, return a list of (hashtag, sentiment value)
	def tweetToHashtagSentiment(tweet: String ) : List[(String, Double)] = {
	  val sentiment: Double = TweetUtilities.getSentiment(tweet)
	  
	  val lb: ListBuffer[(String, Double)] = ListBuffer()
	
	  for (ht <- TweetUtilities.getHashTags(tweet)) {
	      lb += ((ht, sentiment))
	  }
	  return lb.toList
	}

	// For each tweet, return a list of (mention, sentiment value)
	def tweetToMentionSentiment(tweet: String ) : List[(String, Double)] = {
	  val sentiment: Double = TweetUtilities.getSentiment(tweet)
	  
	  val lb: ListBuffer[(String, Double)] = ListBuffer()
	
	  for (mention <- TweetUtilities.getMentions(tweet)) {
	      lb += ((mention, sentiment))
	  }
	  return lb.toList
	}
	
	def myAverage(buf: Iterable[Double]) : Double = {
	  var mysum = 0.0
    buf.foreach(mysum += _)
    return mysum / buf.size
	}

 	def main(args: Array[String]): Unit = {
	 	println("Twitter data batch processing")
		val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")
		
		// 2.1. Number of "Donald Trump" mentions
		val donaldTweets: RDD[String] = tweets.filter(text => text.contains("Trump"))
		println("==== 'Trump' mentions in tweets ====")
		println(donaldTweets.count())
		// val donaldCount: Long = tweets.filter(text => text.contains("Donald Trump")).count()
		// println(donaldCount)

		// 2.2. RDD (tweet, sentiment)
		val tweetSentiments: RDD[(String, Double)] = tweets.map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
		tweetSentiments.take(5).foreach(println)
		
		// 2.3. Sentiments associated with each hashtags: average of tweet sentiment values associated with each hashtag
		println("==== Hashtags Sentiments ====")
		val hashtagSentiment: RDD[(String, Double)] = tweets.flatMap(tw => tweetToHashtagSentiment(tw))
		val hashtagGrouped = hashtagSentiment.groupByKey()
		val hashtagSentimentFinal = hashtagGrouped.map(x => (x._1, myAverage(x._2)))
		//val hashtagSentimentFinal = hashtagGrouped.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
		hashtagSentimentFinal.take(10).foreach(println)
		
		// 2.4 most positive/negative hashtags 
		println("==== Most positive hashtags ====")		
		hashtagSentimentFinal.sortBy(_._2, ascending=false).take(10).foreach(println)
		println("==== Most negative hashtags ====")		
		hashtagSentimentFinal.sortBy(_._2, ascending=true).take(10).foreach(println)

		// 2.3/2.4 bis Most positive mentions
		val mentionSentiment: RDD[(String, Double)] = tweets.flatMap(tw => tweetToMentionSentiment(tw))
		val mentionGrouped = mentionSentiment.groupByKey()
		val mentionSentimentFinal = mentionGrouped.map(x => (x._1, myAverage(x._2)))
		println("==== Most positive mentions ====")		
		mentionSentimentFinal.sortBy(_._2, ascending=false).take(10).foreach(println)
		println("==== Most negative mentions ====")		
		mentionSentimentFinal.sortBy(_._2, ascending=true).take(10).foreach(println)
		
		val test: RDD[String] = tweets.filter(text => text.contains("katesulf"))
		val test2: RDD[(String, Double)] = test.map(tweet => (tweet, TweetUtilities.getSentiment(tweet)))
		test2.take(10).foreach(println)
  }
}
