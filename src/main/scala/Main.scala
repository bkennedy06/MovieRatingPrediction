import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("Starting the KNN Movie Rating Prediction pipeline...")

    // Start SparkSession
    val spark = SparkSession.builder()
      .appName("KNN Movie Rating Prediction")
      .master("local[*]") // Run locally
      .getOrCreate()

    try {
      val processedData = Preprocess.preprocess(spark)
      println(s"Preprocessing completed. ${processedData.count()} records processed.")

      val predictions = TrainModel.run(processedData)
      println(s"Training completed. Predictions generated for ${predictions.count()} movies.")

      EvalModel.evaluate(predictions)

      println("Pipeline done")
    } catch {
      case e: Exception =>
        println(s"Error during execution: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
