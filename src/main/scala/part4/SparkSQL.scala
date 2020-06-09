package part4

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()


  // we can run ANY SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String]) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-12-31' and hire_date < '2001-01-02'""".stripMargin).show()

  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees as e, dept_emp as de, salaries as s
      |where e.hire_date > '1999-12-31' and e.hire_date < '2001-01-02'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no""".stripMargin).show()

  spark.sql(
    """
      |select d.dept_name, max(s.salary)
      |from employees as e, dept_emp as de, departments as d, salaries as s
      |where hire_date > '1999-12-31' and hire_date < '2001-01-02'
      | and e.emp_no = de.emp_no
      | and de.dept_no = d.dept_no
      | and e.emp_no = s.emp_no
      |group by d.dept_name""".stripMargin).show

}
