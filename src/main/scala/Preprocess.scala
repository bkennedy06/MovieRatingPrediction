import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Preprocess {

  def preprocess(spark: SparkSession): DataFrame = {
    val titles = spark.read.option("sep", "\t").option("header", "true").csv("data/title.basics.tsv")
    val ratings = spark.read.option("sep", "\t").option("header", "true").csv("data/title.ratings.tsv")
    val joinedData = titles.join(ratings, Seq("tconst"))

    joinedData
      .filter(col("titleType") === "movie")
      .filter(col("isAdult") === "0")
      .filter(
        col("runtimeMinutes").isNotNull &&
          col("genres").isNotNull &&
          col("averageRating").isNotNull &&
          col("numVotes") > 50 // Arbitrary number to reduce less popular entries
      )
      .withColumn("runtimeMinutes", col("runtimeMinutes").cast("double"))
  }

// Just for induvidual testing. Remove for final product
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PreprocessorApp")
      .master("local[4]")
      .getOrCreate()

    val cleanedData = preprocess(spark)
    cleanedData.show()
  }
}
