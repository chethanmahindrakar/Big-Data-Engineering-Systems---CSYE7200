// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import requests._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import ujson._

val spark = SparkSession
  .builder()
  .appName("Spark Assignment 2")
  .getOrCreate()

// COMMAND ----------

//Fetching the playlist information
val token = "BQAwW9dc7D6igOkr-DaHPuf3Dku2PoCOibMD7CAJnIojPPhz1tidliF-4BrFIhVjwWySEUdFRheS9TXPufrmejFdFD4uzGgML53laKL75nzT8zdtQdn6-X8yIKS4eqLXXY3jpBjz9Qj5RsjNJHDotEK4kNniWiCRLbJIbP4ovfT2OXgz3zKCkxVit3CdtwCyWPa-Mgar3cJd7-_RGtw8CNx7rLz4NgJkiudjWxHuOQRqP8mwapXHlA2e7pCcMmihOTFsSVwpVIlqbpLYrgQJ3OLtAa9OzTtfjEU2q-LS3fp7c5KDLgE6n18vDFnVo0-tRr4tF3OnVuTCsA"
val headers = Map("Authorization" -> s"Bearer $token")
val limit = 100
val offsets = List.range(0, 500, limit)
val urlCalls = offsets.map(offset => s"https://api.spotify.com/v1/playlists/5Rrf7mqN8uus2AaQQQNdc1/tracks?offset=$offset&limit=$limit")
val req = urlCalls.map(url => requests.get(url, headers = headers))

// COMMAND ----------

//Need to get information about each of the tracks. Using json parsing 
val playlistJson = req.map(response => ujson.read(response.text))
val tracks = playlistJson.flatMap(json => json("items").arr)

// COMMAND ----------

//Top 10 longest tracks information
val longestTracks = tracks.sortBy(track => - (track("track")("duration_ms").num.toLong)).take(10)
val longestTrackDurations = longestTracks.map(track => track("track")("duration_ms").num.toLong)
val top10 = longestTracks.zip(longestTrackDurations)

// COMMAND ----------

//Displaying the top 10 longest songs in the playlist
println("The top 10 longest songs in the playlist are :")
top10.foreach { case (track, durationMs) =>
  println(track("track")("name").str + s" ----- $durationMs ms")
}

// COMMAND ----------

//Getting information about the artists using another the API call
val artistIds = longestTracks.flatMap(track => track("track")("artists").arr.map(artist => artist("id").str)).distinct
val artistDetails = artistIds.map { artistId =>
  val artistRequest = requests.get(s"https://api.spotify.com/v1/artists/$artistId", headers = headers)
  ujson.read(artistRequest.text)
}
val sortedArtistDetails = artistDetails.sortBy(artist => -(artist("followers")("total").num.toLong))

// COMMAND ----------

//Displaying the artist information from the top 10 longest songs in the playlist based on their follower count 
println("\nArtist details of artists in top 10 longest songs:")
sortedArtistDetails.foreach { artist =>
  println(artist("name").str + " ----- " + artist("followers")("total").num.toLong + " followers")
}

// COMMAND ----------


