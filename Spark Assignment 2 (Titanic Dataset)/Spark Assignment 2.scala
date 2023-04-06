// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml._

val spark = SparkSession
  .builder()
  .appName("Spark Assignment 2 - Titanic Dataset ")
  .getOrCreate()



// COMMAND ----------

//Reading the Training Dataset
val df_train = spark.read.option("header",true).option("inferSchema",true).csv("/FileStore/tables/train-2.csv")
df_train.show()

//Reading the Testing Dataset
val df_test = spark.read.option("header",true).option("inferSchema",true).csv("/FileStore/tables/test.csv")
df_test.show()

// COMMAND ----------

import spark.implicits._
//EXPLORATORY DATA ANALYSIS
// Calculate basic statistics
df_train.describe().show()

// Count missing values
df_train.select(df_train.columns.map(colName => sum(col(colName).isNull.cast("Int")).alias(colName)): _*).show()

// Pclass distribution
df_train.groupBy("Pclass").count().show()

// Sex distribution
df_train.groupBy("Sex").count().show()

// Embarked distribution
df_train.groupBy("Embarked").count().show()

// Fare distribution
df_train.select("Fare").summary().show()

// Distribution of Siblings/Spouses and Parents/Children
df_train.select("SibSp", "Parch").summary().show()

// Survival rate by SibSp
df_train.groupBy("SibSp").agg(avg("Survived")).show()

// Survival rate by Parch
df_train.groupBy("Parch").agg(avg("Survived")).show()

// Average age and fare by Pclass
df_train.groupBy("Pclass").agg(avg("Age"), avg("Fare")).show()


// COMMAND ----------

// Correlation between continuous variables and Survival
// Pairwise correlation
val continuousVariables = Seq("Age", "Fare", "SibSp", "Parch")
val correlations = for {
  x <- continuousVariables
  y <- continuousVariables
  if x != y
} yield (x, y, df_train.stat.corr(x, y))

correlations.foreach { case (col1, col2, corr) =>
  println(s"Correlation between $col1 and $col2: $corr")
}

// COMMAND ----------

//GRAPHING

//Age distribution and survival
val ageSurvivalData = df_train
  .select("Age", "Survived")
  .na.drop()
  .withColumn("AgeGroup", ((col("Age") / 10).cast("Int") * 10).cast("String"))
  .groupBy("AgeGroup", "Survived")
  .count()
  .orderBy("AgeGroup", "Survived")

display(ageSurvivalData)


// COMMAND ----------

//Survival Rate based on gender
val genderSurvivalData = df_train.groupBy("Sex", "Survived").count()
display(genderSurvivalData)

// COMMAND ----------

//Feature Engineering
//import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, OneHotEncoder, Bucketizer}
import org.apache.spark.sql.{DataFrame}

    // index embarked column
    def embarkedIndexer = (df: DataFrame) => {
      val indexer = new StringIndexer()
        .setInputCol("Embarked")
        .setOutputCol("Embarked_Indexed")
        .setHandleInvalid("skip")
      indexer.fit(df).transform(df)
    }

    // index pclass column
    def pclassIndexer = (df: DataFrame) => {
      val indexer = new StringIndexer()
        .setInputCol("Pclass")
        .setOutputCol("Pclass_Indexed")
      indexer.fit(df).transform(df)
    }

    // create age buckets
    def ageBucketizer = (df: DataFrame) => {
      val ageSplits = Array(0.0, 5.0, 18.0, 35.0, 60.0, 150.0)
      val bucketizer = new Bucketizer()
        .setInputCol("Age")
        .setOutputCol("AgeGroup")
        .setSplits(ageSplits)
      bucketizer.transform(df)
    }

    // create fare buckets
    def fareBucketizer = (df: DataFrame) => {
      val fareSplits = Array(0.0, 10.0, 20.0, 30.0, 50.0, 100.0, 1000.0)
      val bucketizer = new Bucketizer()
        .setInputCol("Fare")
        .setOutputCol("FareGroup")
        .setSplits(fareSplits)
      bucketizer.transform(df)
    }

    // convert sex to binary
    def sexBinerizer = (df: DataFrame) => {
      df.withColumn("Male", when(df("Sex").equalTo("male"), 1).otherwise(0))
        .drop("Sex")
    }

    // create family size field
    def familySizeCreator = (df: DataFrame) => {
      val inputData = df.withColumn("FamilyMembers", df("Parch") + df("SibSp"))
      val outputData = inputData.withColumn("FamilySize",
        when(inputData("FamilyMembers") === 0, 1)
          .when(inputData("FamilyMembers") > 0 && inputData("FamilyMembers") < 4, 2)
          .otherwise(3))
      outputData}


  

// COMMAND ----------

  // Fill missing values with the mean
  val ageMeanTrain = df_train.agg(avg("Age")).first()(0).asInstanceOf[Double]
  val fareMeanTrain = df_train.agg(avg("Fare")).first()(0).asInstanceOf[Double]
  val trainFilled = df_train.na.fill(ageMeanTrain, Seq("Age")).na.fill(fareMeanTrain, Seq("Fare"))

  val ageMeanTest = df_test.agg(avg("Age")).first()(0).asInstanceOf[Double]
  val fareMeanTest = df_test.agg(avg("Fare")).first()(0).asInstanceOf[Double]
  val testFilled = df_test.na.fill(ageMeanTest, Seq("Age")).na.fill(fareMeanTest, Seq("Fare"))


// call all functions on df_train and df_test
val df_train_fe = fareBucketizer(
  ageBucketizer(
    familySizeCreator(
      sexBinerizer(
        pclassIndexer(
          embarkedIndexer(trainFilled)
        )
      )
    )
  )
)

val df_test_fe = fareBucketizer(
  ageBucketizer(
    familySizeCreator(
      sexBinerizer(
        pclassIndexer(
          embarkedIndexer(testFilled)
        )
      )
    )
  )
)

// one-hot encode relevant columns
val oneHotEncoder = new OneHotEncoder()
  .setInputCols(Array("Embarked_Indexed", "Pclass_Indexed", "FamilySize", "FareGroup", "AgeGroup"))
  .setOutputCols(Array("Embarked_Indexed_Vec", "Pclass_Indexed_Vec", "FamilySize_Vec", "FareGroup_Vec", "AgeGroup_Vec"))

  // Fit the one-hot encoder to the training data
val oneHotModel_train = oneHotEncoder.fit(df_train_fe)
val oneHotModel_test = oneHotEncoder.fit(df_test_fe)

val df_train_ohe = oneHotModel_train.transform(df_train_fe)
val df_test_ohe = oneHotModel_test.transform(df_test_fe)



// COMMAND ----------

//df_train_ohe.show()
//df_test_ohe.printSchema()
df_train_ohe.printSchema()


// COMMAND ----------

//Machine Learning

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

// Define the feature columns to use in the model
val featureCols = Array("Pclass_Indexed_Vec", "Embarked_Indexed_Vec", "FamilySize_Vec", "FareGroup_Vec", "AgeGroup_Vec" , "Male")

// Assemble the features into a single vector column
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
//val df_train_assembled = assembler.transform(df_train_ohe)
//val df_test_assembled = assembler.transform(df_test_ohe)

// Split the training data into training and validation sets for cross-validation
val Array(trainingData, validationData) = df_train_assembled.randomSplit(Array(0.8, 0.2))



// COMMAND ----------

// Prepare features
val indexer = new StringIndexer()
  .setInputCol("Survived")
  .setOutputCol("label")

// Create a Random Forest Classifier model
val rf = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(6)

// Create a pipeline
val pipeline = new Pipeline()
  .setStages(Array(indexer, assembler, rf))

// Train the model
val model = pipeline.fit(trainingData)


// COMMAND ----------

// Make predictions on the test set
val predictions = model.transform(validationData)

// Evaluate the model using the area under the ROC curve
val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("rawPrediction")
  .setMetricName("areaUnderROC")

val roc = evaluator.evaluate(predictions)
println(s"Area under ROC curve = $roc")

// COMMAND ----------

//For the Testing data - predictions made 
// Assemble the features into a single vector column
val assembler1 = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("prediction_survival")
val df_test_assembled = assembler1.transform(df_test_ohe)

// Make predictions on the test set
val predictions_test = model.transform(df_test_assembled)

// COMMAND ----------

predictions_test.select("PassengerId", "prediction").show()
predictions_test.select("PassengerId", "prediction")
  .coalesce(1)
  .write.format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save("/FileStore/tables/prediction.csv")

// COMMAND ----------


