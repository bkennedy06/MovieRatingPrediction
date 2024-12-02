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
    //val binaryGenreDF = uniqueGenres.foldLeft(splitGenresDF) { (df, genre) =>
    //  df.withColumn(genre, array_contains(col("genreArray"), genre).cast("int"))
    //}

    // Genre array not necessary
    //val finalDF = binaryGenreDF.drop("genreArray")

    // Write the final DataFrame to the /data folder so we don't need to rerun
    //finalDF.write.mode("overwrite").parquet("data/processed_movies.parquet")

    //finalDF
    // Get genres as separate columns
    val genres = Array("genre1", "genre2", "genre3")
    val sqlExpr = genres.zipWithIndex.map { case (alias, idx) => col("genreArray").getItem(idx).as(alias) }
    val separateGenresDF = splitGenresDF.select(sqlExpr: _*)

    // Turn genre strings into numbers
    val getIndex = udf((s: String) => uniqueGenres.indexOf(s) + 1)

    val x1 = getIndex.apply(col("genre1")) // creates the new column
    val x2 = getIndex.apply(col("genre2"))
    val x3 = getIndex.apply(col("genre3"))
    val vectorGenresDF = separateGenresDF
      .withColumn("x1", x1) // adds the new column to original DF
      .withColumn("x2", x2)
      .withColumn("x3", x3)
      .drop("genre1", "genre2", "genre3")

    // Merge genres DF with rest of data
    val tempDF1 = splitGenresDF.withColumn("id1", monotonically_increasing_id())
    val tempDF2 = vectorGenresDF.withColumn("id2", monotonically_increasing_id())
    val vectorFinalDF = tempDF1.join(tempDF2, col("id1") === col("id2"), "inner")
      .drop("id1", "id2", "genres", "genreArray")
    vectorFinalDF.write.mode("overwrite").parquet("data/processed_movies.parquet")
    vectorFinalDF
  }

  // Just for individual testing. Remove for final product
  // Takes a bit for the parquet to show up in folder
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PreprocessorApp")
      .master("local[4]")
      .getOrCreate()

    val cleanedData = preprocess(spark)
    cleanedData.show(20, truncate = false)
  }
}
