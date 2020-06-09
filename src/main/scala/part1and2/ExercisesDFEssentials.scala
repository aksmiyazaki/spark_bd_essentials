package part1and2

import org.apache.spark.sql.SparkSession

object ExercisesDFEssentials extends App {
  val spark = SparkSession.builder()
    .appName("Exercise 1")
    .master("local[*]")
    .getOrCreate()

  val smf = Seq(
    ("Xiaomi", "Mi A3", "6\"", 10),
    ("Samsung", "S10", "6\"", 15),
    ("Motorola", "Moto G22", "5.5\"", 10)
  )

  import spark.implicits._
  val df1 = smf.toDF("Make", "Model", "Screen Size", "Camera MP")
  df1.show()
  df1.printSchema()

  val df2 = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  df2.show()
  df2.printSchema()
  println(s"Rows in df: ${df2.count()}")
}
