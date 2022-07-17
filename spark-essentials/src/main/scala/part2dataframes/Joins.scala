package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}
import part2dataframes.DataSources.spark

object Joins extends App {
  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  //joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF =
    guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()
  //outer joins
  //left outer joins = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()

  //right outer joins = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()

  //outer join = everything in the inner join + all the rows in BOTH table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  //semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  //anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  //things to bear in mind
  //guitaristsBandsDF.select("id", "band").show() this crashes

  //option 1 -rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  //option 2- drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  //option 3-rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF
    .join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))
    .show()

  /**
    * Exercises
    - Show all employees and their max salary
    - Show all employees who were never managers
    - find the job titles of the best of the best paid 10 employees
    */
  // Reading from a remote DB

  def readTable(name: String) =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$name")
      .load()

  val employeesDF = readTable("employees")

  val salariesDF = readTable("salaries")

  val titlesDF = readTable("titles")

  val deptManagersDF = readTable("dept_manager")
  // exercise 1: Show all employees and their max salary
  val maxSalariesPerEmployeeDF = salariesDF.groupBy("emp_no").max("salary")

  val employeesSalariesDF = employeesDF
    .join(maxSalariesPerEmployeeDF, "emp_no")

  employeesSalariesDF.show()

  // exercise 2: Show all employees who were never managers

  val neverManagersDF = employeesDF.join(deptManagersDF,
                                         employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
                                         "left_anti")

  neverManagersDF.show()
  // exercise 3: find the job titles of the best of the best paid 10 employees

  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF   = employeesSalariesDF.orderBy(col("max(salary)").desc).limit(10)
  val bestPaidEmployeesTitlesDF = bestPaidEmployeesDF
    .join(mostRecentJobTitlesDF, "emp_no")

  bestPaidEmployeesTitlesDF.show()

}
