
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf

object MovieRatingAnalyzer {
  // Calculate the mean rating and standard deviation for each movies
  def calculateMovieStats(movieRatingsDF: DataFrame): DataFrame = {
    movieRatingsDF.groupBy("movieId")
      .agg(
        mean("rating").alias("meanRating"),
        stddev("rating").alias("stdDevRating")
      )
  }

  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("MovieRatingAnalyzerSpec")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val movieRatingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:/Masters Courses/Big Data Engineering/CSYE7200-Spring2023/CSYE7200-Spring2023/Big-Data-Engineering-Systems---CSYE7200/Assignment 7 - Analyzing movie Rating/src/main/resources/ratings_small.csv")

    //Calculate the mean rating and standard ddeviation for all movies
    val single_mean = movieRatingsDF.select(avg("rating").alias("meanRating"))
    val single_stddev = movieRatingsDF.select(stddev("rating").alias("stdDevRating"))
    //println(single_mean.show())
    //println(single_stddev.show())

    // Write results for each movie
    calculateMovieStats(movieRatingsDF).write
      .option("header",true).csv("D:\\Masters Courses\\Big Data Engineering\\CSYE7200-Spring2023\\CSYE7200-Spring2023\\Big-Data-Engineering-Systems---CSYE7200\\Assignment 7 - Analyzing movie Rating\\Output\\meanAndStddev.csv")

    //Write results for all movies
    val combinedDF = single_mean.join(single_stddev)

    combinedDF.write
      .option("header", "true")
      .option("delimiter", ",")
      .csv("D:\\Masters Courses\\Big Data Engineering\\CSYE7200-Spring2023\\CSYE7200-Spring2023\\Big-Data-Engineering-Systems---CSYE7200\\Assignment 7 - Analyzing movie Rating\\Output\\singlular_output.csv")

    // Stop the SparkSession
    spark.stop()
  }
}