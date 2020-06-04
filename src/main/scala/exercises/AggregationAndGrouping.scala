package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationAndGrouping extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping Exercise")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF
    .selectExpr(
      "US_Gross",
      "Worldwide_Gross",
      "US_DVD_Sales",
      "US_Gross + Worldwide_Gross + US_DVD_Sales as Profit"
    )
    .na
    .fill(0)
    .select(sum("Profit"))
    .show()

  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  moviesDF
    .select(
      avg(col("US_Gross")),
      stddev(col("US_Gross"))
    ).show()

  moviesDF
    .groupBy("Director")
    .agg(
      avg(col("IMDB_Rating")).as("avgRating"),
      sum(col("US_Gross")).as("totalGross")
    ).orderBy(col("totalGross").desc_nulls_last)
    .show()
}
