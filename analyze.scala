import org.apache.spark.sql.SparkSession

object TweetAnalyzer {

  def main(args: Array[String]): Unit = {

    // Create a Spark session
    val spark = SparkSession.builder()
      .appName("TweetAnalyzer")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    // Read the tweet data from Hive
    val tweets = spark.sql("SELECT * FROM tweets")

    // Register the tweet data as a temporary view
    tweets.createOrReplaceTempView("tweets")

    // Analyze the tweet data using Spark SQL
    // For example, count the number of tweets by language
    val tweetCountByLang = spark.sql("SELECT lang, COUNT(*) AS count FROM tweets GROUP BY lang ORDER BY count DESC")

    // Show the result
    tweetCountByLang.show()

    // Write the result to Hive
    tweetCountByLang.write.mode("overwrite").saveAsTable("tweet_count_by_lang")
  }
}
