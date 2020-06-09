package part3

import java.sql.Date

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object DataSets extends App {
  val spark = SparkSession.builder()
    .appName("Dataset Exercise")
    .config("spark.master", "local[*]")
    .getOrCreate()

  case class Car(
                Name: String,
                Miles_per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Year: Date,
                Origin: String
                )

  val carsDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/cars.json")

  import spark.implicits._
  val carsDS: Dataset[Car] = carsDF.as[Car]

  println("Total Cars")
  val totalCars = carsDS.count()
  println(totalCars)

  println("Powerfull Cars")
  println(carsDS.filter(
    _.Horsepower match {
      case Some(hp) => hp > 140
      case None => false
    }
  ).count())

  println("Avg HP")
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / totalCars)
  carsDS.select(avg("Horsepower")).show()
}
