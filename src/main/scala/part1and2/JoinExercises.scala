package part1and2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max, first, last}
import org.apache.spark.sql.types._

object JoinExercises extends App {
  val spark = SparkSession.builder()
    .appName("Joins Exercises")
    .config("spark.master", "local[*]")
    .getOrCreate()

  def fromDatabase(tableName: String, schema: StructType = null) = {
    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/rtjvm"
    val user = "docker"
    val password = "docker"

    if (schema != null) {
      spark.read
        .format("jdbc")
        .schema(schema)
        .option("dateFormat", "YYYY-MM-dd")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", tableName)
        .load
    } else {
      spark.read
        .format("jdbc")
        .option("inferSchema", "true")
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("dbtable", tableName)
        .load
    }
  }

  val employeesDF = fromDatabase("public.employees")
  val salaryDF = fromDatabase("public.salaries")

  val groupedSalary = salaryDF
    .groupBy("emp_no")
    .max("salary")

  val empSalDF = employeesDF
    .join(groupedSalary, employeesDF.col("emp_no") === groupedSalary.col("emp_no"))
    .drop(groupedSalary.col("emp_no"))


  empSalDF.show

  val managersDF = fromDatabase("public.dept_manager")

  val employeesNotManagers = employeesDF
    .join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti")

  println(employeesDF.count)
  println(employeesNotManagers.count)

  val titlesSchema = StructType(Array(
    StructField("emp_no", IntegerType),
    StructField("title", StringType),
    StructField("from_date", DateType),
    StructField("to_date", DateType)
  ))

  val titlesDF = fromDatabase("public.titles", titlesSchema)

  val bestPaidEmployees = empSalDF.
    orderBy(col("max(salary)").desc_nulls_last).limit(10)

  val lastTitles = titlesDF
    .orderBy(titlesDF.col("emp_no"), titlesDF.col("to_date"))
    .groupBy("emp_no")
    .agg(
      last("title"),
      max("to_date")
    )

  lastTitles.show

  bestPaidEmployees
    .join(lastTitles, bestPaidEmployees.col("emp_no") === lastTitles.col("emp_no"), "left_outer")
    .drop(lastTitles.col("emp_no"))
    .show
}
