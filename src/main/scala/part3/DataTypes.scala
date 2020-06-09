package part3

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataTypes extends App {
  val spark = SparkSession.builder()
    .appName("Exercise Data Types")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  def getCarNames(df: DataFrame, list: List[String]) = {
    val regexStr = list.mkString("|")
    println(s"Filtering by ${regexStr}")

    df.select(
      col("*"),
      regexp_extract(col("Name"), regexStr, 0).as("filteredContent")
    ).where(col("filteredContent") =!= "")
  }

  def getCarNames2(df: DataFrame, list: List[String]) = {
    val regexStr = list.mkString("|")
    println(s"Filtering by ${regexStr}")

    df.filter(regexp_extract(col("Name"), regexStr, 0) =!= "")
  }

  def getCarWithContains(df: DataFrame, list: List[String]) = {
    val lsOfContains = for (c <- list) yield col("Name").contains(c)

    lsOfContains
      .map(x => df.filter(x))
      .reduce(_ union _)
  }

  val res = getCarNames(carsDF, List("ford", "vw"))
  res.show()
  println(res.count)

  val resContains = getCarWithContains(carsDF, List("ford", "vw"))
  res.show()
  println(res.count)

}
