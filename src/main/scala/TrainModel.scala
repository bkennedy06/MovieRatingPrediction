import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions._

object TrainModel {
  def run(processedDf: DataFrame): DataFrame = {
    // Drop rows with null values in these columns
    val nonNullData = processedDf.na.drop(Seq("runtimeMinutes", "x1", "x2", "x3", "averageRating"))

    // Cast averageRating to double
    val numericDf = nonNullData.withColumn("averageRating", col("averageRating").cast("double"))

    // Assemble into a single column
    val assembler = new VectorAssembler()
      .setInputCols(Array("runtimeMinutes", "x1", "x2", "x3"))
      .setOutputCol("features")

    val assembledDf = assembler.transform(numericDf).select("features", "averageRating")

    // Train a linear regression model
    val lr = new LinearRegression()
      .setLabelCol("averageRating")
      .setFeaturesCol("features")

    val lrModel = lr.fit(assembledDf)

    val predictions = lrModel.transform(assembledDf)
    predictions.write.mode("overwrite").parquet("data/predictions.parquet")
    predictions
  }

  // More individual testing. Make sure you've run Preprocess first and .parquet is in /data
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("TrainModel")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val processedDf = spark.read.parquet("data/processed_movies.parquet")

    val predictions = run(processedDf)

    // Shows 20 rows for testing
    predictions.select("features", "averageRating", "prediction").show(20, truncate = false)

    spark.stop()
  }
}
