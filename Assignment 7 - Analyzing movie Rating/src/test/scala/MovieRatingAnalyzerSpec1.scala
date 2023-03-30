import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.collection.mutable.ArraySeq
import org.apache.spark.sql.Row


class MovieRatingAnalyzerSpec extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder()
    .appName("MovieRatingAnalyzerSpec")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val movieRatingsDF = Seq(
    (1, 4.0),
    (1, 3.0),
    (2, 2.0),
    (2, 2.5),
    (2, 3.0)
  ).toDF("movieId", "rating")



  it should "return an empty DataFrame if the input DataFrame is empty" in {
    val emptyDF = Seq.empty[(Int, Double)].toDF("movieId", "rating")
    val movieStatsDF = MovieRatingAnalyzer.calculateMovieStats(emptyDF)

    movieStatsDF.collect() should be(empty)
  }

  it should "return the same results regardless of the order of the input DataFrame" in {
    val shuffledDF = movieRatingsDF.orderBy($"rating".desc)
    val movieStatsDF1 = MovieRatingAnalyzer.calculateMovieStats(movieRatingsDF)
    val movieStatsDF2 = MovieRatingAnalyzer.calculateMovieStats(shuffledDF)

    movieStatsDF1.collect() should contain theSameElementsAs movieStatsDF2.collect()
  }
}