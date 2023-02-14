// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession
  .builder()
  .appName("Spark Assignment 1")
  .getOrCreate()

// COMMAND ----------

//Reading the Dataset
val df = spark.read.option("header",true).option("inferSchema",true).csv("/FileStore/tables/train_my.csv")
df.show()

// COMMAND ----------

//Sub-question 1 -------- The average ticket fare for each ticket class
df.filter("Pclass = 1").agg(avg("Fare").as("Upper Average")).show()
df.filter("Pclass = 2").agg(avg("Fare").as("Middle Average")).show()
df.filter("Pclass = 3").agg(avg("Fare").as("Lower Average")).show()


// COMMAND ----------

df.groupBy("Pclass").agg(avg("Fare")).orderBy("Pclass").show()

// COMMAND ----------

//Sub Question 2 ------- Survival Percentage for each Ticket class
val totalPassenger = df.select("PassengerId").count()
val survived1 : Double = (df.filter("Pclass = 1").filter("Survived = 1").count().toDouble / totalPassenger.toDouble) * 100 
val survived2 : Double = (df.filter("Pclass = 2").filter("Survived = 1").count().toDouble / totalPassenger.toDouble) * 100
val survived3 : Double = (df.filter("Pclass = 3").filter("Survived = 1").count().toDouble / totalPassenger.toDouble) * 100

import scala.math._
print(max(survived1, max(survived2, survived3)))


// COMMAND ----------

//Sub Question 3 --------- find the number of passengers who could possibly be Rose
// Survival = 1, Pclass = 1, Parch = 1, Gender = Female, age = 17

//df.filter("Pclass = 1").filter("Survived = 1").filter("sex = 'female'").filter("Parch = 1").filter("age = 17").count
df.filter(expr("sex = 'female' and Survived = 1 and Pclass = 1 and Parch = 1 and age = 17")).count

// COMMAND ----------

//Sub Question 4 --------- Number of passengers who could possibly be Jack?
//Survival = 0, Pclass = 3, Gender = Male, age = 19 or 20, SibSp = 0, Parch = 0
var temp_df = df.filter(expr("sex = 'male'  and Pclass = 3 and SibSP = 0 and Parch = 0 and survived = 0"))
temp_df = temp_df.filter(expr("age >= 19 and age <= 20"))
//temp_df.count()

df.filter(expr("(sex = 'male'  and Pclass = 3 and SibSP = 0 and Parch = 0 and survived = 0) and (age >= 19 and age <= 20)" )).count()


// COMMAND ----------

//Sub Question 4 ------- Continued --- Displaying potential Jacks
temp_df.show(22)  //Since show only displays the first 20 rows

// COMMAND ----------

//Sub Question 5 ---------- Split the age across every 10 years.

val agedf = df.withColumn(
  "age",
  when(col("age").between(1, 10), "1-10").
  when(col("age").between(11, 20), "11-20").
  when(col("age").between(21, 30), "21-30").
  when(col("age").between(31, 40), "31-40").
  when(col("age").between(41, 50), "41-50").
  when(col("age").between(51, 60), "51-60").
  when(col("age").between(61, 70), "61-70").
  when(col("age").between(71, 80), "71-80").
  when(col("age").between(81, 90), "81-90").
  otherwise("other")).toDF

// "other" over here corresponds to all the null values that are in the dataset


// COMMAND ----------

// Relationship between age and ticket fare -- Need to calculate the mean to get a rough idea
agedf.groupBy("age").agg(avg("Fare")).orderBy("age").show()  // -- Avg price for ages 1 - 30 is about 29 whereas that for ages b/n 40-70 is 42

// COMMAND ----------

//Which age group most likely survived? -- Count the number of survived in each group

//agedf.groupBy("age").agg(sum(when(col("survived") === true, 1)).alias("survived_count")).orderBy(desc("survived_count")).show
val tempAgeDf = agedf.groupBy("age").agg(sum(when(col("survived") === true, 1)).alias("survived_count")).orderBy(desc("survived_count"))
tempAgeDf.first() //Most survived age group is 21-30




// COMMAND ----------


