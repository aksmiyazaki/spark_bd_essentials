package part5

import org.apache.spark.sql.{SQLContext, SparkSession}

object RddExercises extends App {
  val spark = SparkSession.builder()
    .appName("RDD Exercises")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  case class Movie(title: String, genre: String, rating: Double)

  def extractBasedOnPrefix(row: String, prefix: String, isDouble: Boolean = false) = {
    val pattern =
      if (isDouble) {
        ("\"" + prefix + "\":([\\d*.\\d*])").r
      } else {
        ("\"" + prefix + "\":\"([\\w\\d\\s.,!?']*)\"").r
      }

    val res = (pattern findFirstIn row).getOrElse("")
    res match {
      case "" | "null" => ""
      case _ => res.split(":")(1).stripPrefix("\"").stripSuffix("\"")
    }
  }

  val moviesRdd = sc.textFile("src/main/resources/data/movies.json")
    .map(line => line.stripMargin('{').stripMargin('}'))
    .map(fields =>
      Movie(
        extractBasedOnPrefix(fields, "Title"),
        extractBasedOnPrefix(fields, "Major_Genre"),
        extractBasedOnPrefix(fields, "IMDB_Rating", true) match {
          case "null" | "" => 0.0
          case _ => extractBasedOnPrefix (fields, "IMDB_Rating", true).toDouble
        }
      )
    )

  moviesRdd.toDF().show(20, false)

  moviesRdd
    .map(x => x.genre)
    .distinct()
    .toDF()
    .show(20, false)

  moviesRdd
    .filter(x => x.genre == "Drama" && x.rating > 6)
    .toDF()
    .show(20, false)

  moviesRdd
    .map(x => (x.genre, x.rating))
    .groupByKey()
    .mapValues{
      x => x.sum / x.size
    }.toDF()
    .show(20, false)

}
