import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Assignment 7 - Movie Rating Analyzer")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val movieRatingsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("D:/Masters Courses/Big Data Engineering/CSYE7200-Spring2023/CSYE7200-Spring2023/Big-Data-Engineering-Systems---CSYE7200/Assignment 7 - Analyzing movie Rating/src/main/resources/movie_metadata.csv")

    //println(movieRatingsDF)
  }
}