package part3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DatasetJoins extends App {
  val spark = SparkSession.builder()
    .appName("Dataset Joins")
    .config("spark.master", "local[*]")
    .getOrCreate()

  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  import spark.implicits._
  val guitarsDS = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitars.json").as[Guitar]
  val guitarPlayersDS = spark.read.option("inferSchema", "true").json("src/main/resources/data/guitarPlayers.json").as[GuitarPlayer]

  val joined = guitarsDS.joinWith(guitarPlayersDS
    ,array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id"))
    , "outer")

  joined.show()


}
