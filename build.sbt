ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17" // Required for Spark 3.5.1

lazy val root = (project in file("."))
  .settings(
    name := "GroupProject369",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
      "org.apache.spark" %% "spark-mllib" % "3.5.1" % Provided // MLlib for KNN
    ),
    Compile / run / fork := true
  )
