package part3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexDataTypes extends App {
  val spark = SparkSession.builder()
    .appName("Exercise Complex Data Types")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val dfWithDateCol = moviesDF
    .select(col("Title"),
      when(to_date(col("Release_Date"), "dd-MMM-yy").isNotNull,
        to_date(col("Release_Date"), "dd-MMM-yy"))
        .when(to_date(col("Release_Date"), "d-MMM-yy").isNotNull,
          to_date(col("Release_Date"), "d-MMM-yy")).as("ParsedDate"))

  dfWithDateCol.show()
  dfWithDateCol.filter(col("ParsedDate").isNull).show()


  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select(
    col("*"),
    to_date(col("date"), "MMM d yyyy").as("ParsedDate")
  ).show

  stocksDF.withColumn("ParsedDate", to_date(col("date"), "MMM d yyyy")).show
}
