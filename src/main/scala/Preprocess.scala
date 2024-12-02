import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Preprocess {

  def preprocess(spark: SparkSession): DataFrame = {
    val titles = spark.read.option("sep", "\t").option("header", "true").csv("data/title.basics.tsv")
    val ratings = spark.read.option("sep", "\t").option("header", "true").csv("data/title.ratings.tsv")

    // Join titles and ratings on tconst
    val joinedData = titles.join(ratings, Seq("tconst"))

    // Filter data for bad entries
    val filteredData = joinedData
      .filter(col("titleType") === "movie")
      .filter(col("isAdult") === "0")
      .filter(
        col("runtimeMinutes").isNotNull &&
          col("genres").isNotNull &&
          col("averageRating").isNotNull &&
          col("numVotes") > 50
      )
      .withColumn("runtimeMinutes", col("runtimeMinutes").cast("double"))

    // Split genres into an array
    val splitGenresDF = filteredData.withColumn("genreArray", split(col("genres"), ","))

    // Get unique ones
    val uniqueGenres = splitGenresDF
      .select(explode(col("genreArray")).alias("genre"))
      .distinct()
      .collect()
      .map(_.getString(0))

    // Create binary columns for em
    val binaryGenreDF = uniqueGenres.foldLeft(splitGenresDF) { (df, genre) =>
      df.withColumn(genre, array_contains(col("genreArray"), genre).cast("int"))
    }

    // Genre array not necessary
    val finalDF = binaryGenreDF.drop("genreArray")

    // Write the final DataFrame to the /data folder so we don't need to rerun
    finalDF.write.mode("overwrite").parquet("data/processed_movies.parquet")

    finalDF
  }

  // Just for individual testing. Remove for final product
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PreprocessorApp")
      .master("local[4]")
      .getOrCreate()

    val cleanedData = preprocess(spark)
    cleanedData.show(20, truncate = false)
  }
}
