1. Preprocess.scala
Purpose: Handle data loading and cleaning.
Should do:
Load IMDb datasets from the data/ directory.
Clean missing or invalid data.
Transform datasets into a format suitable for KNN, such as converting genres and runtime into numerical features.
Output: Returns a processed DataFrame for model training.

2. TrainModel.scala
Purpose: Implements the KNN algorithm to predict movie ratings.
Should do:
Accept the preprocessed DataFrame from Preprocessing.scala.
Compute distances between movies based on features (e.g., genres, runtime, etc.).
Use the nearest neighbors to predict ratings for movies.
Output: Stores the predicted ratings for evaluation.

3. EvalModel.scala
Purpose: Evaluate the accuracy and performance of the KNN predictions.
Should do:
Compare predicted ratings to actual ratings from the dataset.
Calculate evaluation metrics, such as:
Mean Absolute Error (MAE)
Root Mean Square Error (RMSE)
Generate a summary of model performance.

4. Main.scala
Purpose: The main entry point for running the project pipeline.
Should do:
Initialize the Spark session.
Call the functions from Preprocess.scala, TrainModel.scala, and EvalModel.scala in sequence.
Print the results or save outputs for further analysis.
