from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("ElectricityPricePredictionModelTraining").getOrCreate()

# Load Data from HDFS
HDFS_PATH = 'hdfs://hdfs_server:port/path/'
df = spark.read.parquet(HDFS_PATH)

# Convert necessary columns to numeric (in case they're in string format with commas)
for col in ["Temperature", "Wind", "Sunshine", "Rain"]:
    df = df.withColumn(col, F.regexp_replace(F.col(col), ",", ".").cast("float"))

# Select relevant columns for flexibility
columns_needed = ["Temperature", "Wind", "Sunshine", "Rain"]
df = df.select(*columns_needed, "price")

# Set up the feature vector and label column
assembler = VectorAssembler(inputCols=columns_needed, outputCol="features")
pipeline = Pipeline(stages=[assembler])

# Transform the data
prepared_data = pipeline.fit(df).transform(df).select("features", "price")

# Train a Linear Regression Model
lr = LinearRegression(featuresCol="features", labelCol="price")
model = lr.fit(prepared_data)

# Save the model
model.save("hdfs://hdfs_server:port/model/electricity_price_prediction_model")
print("Model training completed and saved.")

spark.stop()
