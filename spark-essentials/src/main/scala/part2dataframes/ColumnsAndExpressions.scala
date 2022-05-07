package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, column, expr, lit, sum}

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting
  val carNamesDF = carsDF.select(firstColumn)

  //various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )
  // EXPRESSIONS
  val simplestExpression   = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  /**
    * Exercises
    * 1. Read the movies DF and select 2 columns of your choice
    2.Create another column up the total profit of the movies = US_GROSS + Worldwide_GROSS + US_DVD_Sales
    3. Select all COMEDY with IMDB rating above 6
    Use as many as possible
    */
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val selectMoviesDF1 = moviesDF.select(col("Title"), col("Director"))
  selectMoviesDF1.show(false)

  val selectMoviesDF2 = moviesDF.select('Title, 'Major_Genre)
  selectMoviesDF2.show(false)

  val selectMoviesDF3 = moviesDF.select(column("Title"), column("Release_Date"))
  selectMoviesDF3.show(false)

  val selectMoviesDF4 = moviesDF.selectExpr("Title", "IMDB_Votes")
  selectMoviesDF4.show()
  val moviesTotalCostDF1 = moviesDF
    .withColumn(
      "total_profit",
      coalesce(col("US_Gross"), lit(0)) + coalesce(col("Worldwide_Gross"), lit(0)) + coalesce(
        col("US_DVD_Sales"),
        lit(0)))
  moviesTotalCostDF1.select('Title, 'total_profit).show(false)

  val moviesTotalCostDF2 = moviesDF
    .selectExpr(
      "Title",
      "US_Gross + Worldwide_Gross as total_profit"
    )
  moviesTotalCostDF2.show(false)

  val comedyDF1 = moviesDF
    .filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6.0)

  comedyDF1.select('Title, 'Major_Genre, 'IMDB_Rating).show(false)

}
