from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("WeatherAndElectricityDataAnalysis").getOrCreate()

try:
    # Paths to the Parquet files on HDFS
    weather_hdfs_path = "hdfs://namenode:9000//historical_data/historical_weather_data.parquet"
    electricity_hdfs_path = "hdfs://namenode:9000/historical_data/historical_el_prices_data.parquet"
    
    # Read the Parquet files
    weather_df = spark.read.parquet(weather_hdfs_path)
    electricity_df = spark.read.parquet(electricity_hdfs_path)
    
    # Display the first few rows of the weather dataset
    print("Weather Data:")
    weather_df.show(100, truncate=False)
    weather_df.printSchema()
    
    # Display the first few rows of the electricity prices dataset
    print("Electricity Prices Data:")
    electricity_df.show(10, truncate=False)
    electricity_df.printSchema()

    # Perform some analysis on Weather Data
    print("Weather Data Analysis:")
    print(f"Total number of rows in weather data: {weather_df.count()}")
    weather_df.select("parameterId").distinct().show()
    weather_df.select("value").describe().show()

    # Perform some analysis on Electricity Prices Data
    print("Electricity Prices Data Analysis:")
    print(f"Total number of rows in electricity prices data: {electricity_df.count()}")
    electricity_df.groupBy("PriceArea").agg({"SpotPriceDKK": "avg"}).show()
    electricity_df.select("SpotPriceDKK", "observedDKK", "observedUTC").describe().show()

except Exception as e:
    print(f"Error reading or analyzing the Parquet files: {e}")

finally:
    spark.stop()
