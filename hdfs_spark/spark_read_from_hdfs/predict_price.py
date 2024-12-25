import sys
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from kafka import KafkaProducer
import json

# Constants
HDFS_MODEL_PATH = "hdfs://namenode:9000/electricity_price_model_with_weather"
KAFKA_BROKER = "kafka:9092"
KAFKA_PREDICTION_TOPIC = "predictions"

# Log the received arguments
print(f"Received arguments: {sys.argv}")

try:
    # Parse arguments
    observed_time = sys.argv[1]
    energy_grid = sys.argv[2]
    weather_values = list(map(float, sys.argv[3:]))

    # Validate inputs
    if len(weather_values) != 4:
        raise ValueError("Expected 4 weather values (humidity, temp_dry, wind_speed, cloud_cover).")
    print(f"Input data: Time={observed_time}, EnergyGrid={energy_grid}, WeatherValues={weather_values}")

except Exception as e:
    print(f"Error parsing inputs: {e}")
    sys.exit(1)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MetObsPrediction") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

producer = None

try:
    # Load model
    print(f"Loading model from {HDFS_MODEL_PATH}...")
    model = LinearRegressionModel.load(HDFS_MODEL_PATH)

    # Prepare data for prediction
    input_df = spark.createDataFrame([(Vectors.dense(weather_values),)], ["features"])
    print(f"DataFrame created: {input_df.show()}")

    # Make prediction
    predictions = model.transform(input_df)
    predicted_price = predictions.collect()[0]["prediction"]
    print(f"Predicted price: {predicted_price}")

    # Prepare Kafka message
    output_message = {
        "time": observed_time,
        "energyGrid": energy_grid,
        "price": predicted_price
    }
    print(f"Output message: {output_message}")

    # Send to Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(KAFKA_PREDICTION_TOPIC, output_message)
    producer.flush()
    print("Prediction sent to Kafka.")

except Exception as e:
    print(f"Error during prediction: {e}")
    import traceback
    traceback.print_exc()

finally:
    if producer:
        producer.close()
    spark.stop()
    print("Spark session stopped.")