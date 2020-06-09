package part1and2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ExercisesColumnsTransformations extends App {
  val spark = SparkSession.builder()
    .appName("Exercises Columns")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")


  val twoColumnDF1 = moviesDF.select("Title", "IMDB_Rating")
  println("DF1")
  twoColumnDF1.show()

  val twoColumnDF2 = moviesDF.select(
    col("Title"),
    column("IMDB_Rating")
  )

  println("DF2")
  twoColumnDF2.show()

  import spark.implicits._
  val twoColumnDF3 = moviesDF.select(
    'Title,
    $"IMDB_Rating"
  )

  println("DF3")
  twoColumnDF3.show()

  val targetDF = moviesDF.select("Title", "US_Gross", "Worldwide_Gross", "US_DVD_Sales").na.fill(0)

  val withProfit1 = targetDF.withColumn("Total Profit",
    col("US_Gross") + col("Worldwide_Gross") + expr("US_DVD_Sales"))

  withProfit1.show()


  val targetDF2 = moviesDF.select("Title", "Major_Genre", "IMDB_Rating").na.fill(0)

  val comedyDF = targetDF2.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  comedyDF.show()

  val comedyDF2 = targetDF2.where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  comedyDF2.show()

  val comedyDF3 = targetDF2.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6")
  comedyDF3.show()
}
