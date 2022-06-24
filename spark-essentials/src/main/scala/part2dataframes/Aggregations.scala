package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{
  approx_count_distinct,
  avg,
  col,
  count,
  countDistinct,
  mean,
  stddev
}

object Aggregations extends App {
  val spark = SparkSession
    .builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")
  // counting all the rows, and will include nulls
  moviesDF.select(count("*")).show
  genresCountDF.show()

  //counting distinct
  moviesDF.select(countDistinct("Major_Genre")).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(functions.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  //sum
  moviesDF.select(functions.sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  //avg
  moviesDF.select(functions.avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF
    .select(
      mean(col("Rotten_Tomatoes_Rating")),
      stddev(col("Rotten_Tomatoes_Rating"))
    )
    .show()

  // Grouping

  val countByGenreDf = moviesDF
    .groupBy(col("Major_Genre"))
    .count() // select count(*) from moviesDF group by Major_Genre
  countByGenreDf.show()

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  avgRatingByGenreDF.show()

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_rating")
    )
    .orderBy(col("Avg_rating"))
  aggregationsByGenreDF.show()

  /*
   * Exercises
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the avg US gross revenue per director
   */
  //1.
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(functions.sum("Total_Gross"))
    .show()
  //2.
  moviesDF.select(countDistinct("Director")).show()
  //3.
  moviesDF.select(mean(col("US_Gross")), stddev(col("US_Gross"))).show()
  //4.
  moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("Avg_rating"),
      avg("US_Gross").as("Avg_revenue")
    )
    .orderBy(col("Avg_rating").desc_nulls_last)
    .show()
}
