package part1and2

import org.apache.spark.sql.{SaveMode, SparkSession}

object ExercisesDataSources extends App {
  val spark = SparkSession.builder()
    .appName("Exercises Data Sources")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("sep", "\t")
    .option("header", "true")
    .csv("src/main/resources/data/moviesduptsv")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/moviesdupparquet")

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  moviesDF.write
    .format("jdbc")
    //.mode(SaveMode.Overwrite)
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
