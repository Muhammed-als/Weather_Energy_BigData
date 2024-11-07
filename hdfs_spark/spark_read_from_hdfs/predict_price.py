from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark Session
spark = SparkSession.builder.appName("ElectricityPricePrediction").getOrCreate()

# Load the trained model
model = LinearRegressionModel.load("hdfs://hdfs_server:port/model/electricity_price_prediction_model")

# Define schema for Kafka input data
schema = StructType([
    StructField("Temperature", StringType(), True),
    StructField("Wind", StringType(), True),
    StructField("Sunshine", StringType(), True),
    StructField("Rain", StringType(), True)
])

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_server:port") \
    .option("subscribe", "weather_topic") \
    .load()

# Parse Kafka data and convert columns to float
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")
for col_name in schema.fieldNames():
    parsed_df = parsed_df.withColumn(col_name, F.regexp_replace(F.col(col_name), ",", ".").cast("float"))

# Assemble features
assembler = VectorAssembler(inputCols=schema.fieldNames(), outputCol="features")
data_for_prediction = assembler.transform(parsed_df)

# Make predictions
predictions = model.transform(data_for_prediction).select("features", "prediction")

# Write predictions to console (or HDFS/Kafka as needed)
query = predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
spark.stop()
