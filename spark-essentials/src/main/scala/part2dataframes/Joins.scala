package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App { /*
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
  val guitaristsBandsDF =
    guitaristDF.join(bandsDF, guitaristDF.col("band") === bandsDF.col("id"), "inner")
  guitaristsBandsDF.show()*/ }
