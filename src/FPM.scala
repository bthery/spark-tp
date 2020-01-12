package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.ml.feature.StopWordsRemover

object FPM {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing using FPM")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    val tweets = sc.textFile("1Mtweets_en.txt").map(_.toLowerCase().replaceAll("""\p{Punct}""","").split("\\s+").toSet).zipWithIndex().toDF("content", "ID")
    tweets.cache
    val fpgrowth = new FPGrowth().setItemsCol("content").setMinSupport(0.001).setMinConfidence(0.1)
    val model = fpgrowth.fit(tweets)
    // Display frequent itemsets.
    model.freqItemsets.show(100)
    // Display generated association rules.
    model.associationRules.show(500)
    sc.stop
  }
}