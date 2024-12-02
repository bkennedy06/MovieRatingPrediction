import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.SparkSession

object EvalModel {
  def evaluate(predictions: DataFrame): Unit = {
    // Adding a residuals column (actual - predicted) for graphing
    val predictionsWithResiduals = predictions.withColumn("residual", col("averageRating") - col("prediction"))

    // Initialize evaluators for different metrics
    val rmseEvaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("averageRating")
      .setPredictionCol("prediction")

    val mseEvaluator = new RegressionEvaluator()
      .setMetricName("mse")
      .setLabelCol("averageRating")
      .setPredictionCol("prediction")

    val r2Evaluator = new RegressionEvaluator()
      .setMetricName("r2")
      .setLabelCol("averageRating")
      .setPredictionCol("prediction")

    val maeEvaluator = new RegressionEvaluator()
      .setMetricName("mae")
      .setLabelCol("averageRating")
      .setPredictionCol("prediction")

    // Compute metrics
    val rmse = rmseEvaluator.evaluate(predictions)
    val mse = mseEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)
    val mae = maeEvaluator.evaluate(predictions)

    println(s"Root Mean Squared Error (RMSE): $rmse")
    println(s"Mean Squared Error (MSE): $mse")
    println(s"R-Squared (R2): $r2")
    println(s"Mean Absolute Error (MAE): $mae")

    // Save residuals for graphing
    predictionsWithResiduals
      .select("residual")
      .write
      .mode("overwrite")
      .csv("data/residuals.csv")
  }

  // Same deal as before
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("EvalModel")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val predictions = spark.read.parquet("data/predictions.parquet")
    evaluate(predictions)
    spark.stop()
  }
}
