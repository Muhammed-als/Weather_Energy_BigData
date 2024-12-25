from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, col, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ElectricityPricePredictionWithWeather") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

try:
    # Paths to datasets
    weather_hdfs_path = "hdfs://namenode:9000/historical_data/historical_weather_data.parquet"
    electricity_hdfs_path = "hdfs://namenode:9000/historical_data/historical_el_prices_data.parquet"
    model_hdfs_path = "hdfs://namenode:9000/electricity_price_model_with_weather"

    # Load weather and electricity datasets
    weather_df = spark.read.parquet(weather_hdfs_path).limit(200000)
    electricity_df = spark.read.parquet(electricity_hdfs_path).limit(200000)

    # Filter weather data to hourly timestamps
    weather_df = weather_df.withColumn("hourly_observed", date_trunc("hour", col("observed")))

    # Pivot weather data to consolidate features by station and timestamp
    weather_features = weather_df.groupBy("hourly_observed", "Egrid").pivot("parameterId").agg(avg("value"))

    # Convert electricity timestamps to match weather format
    electricity_df = electricity_df.withColumn("hourly_observed", date_trunc("hour", col("observedUTC")))

    # Rename PriceArea to Egrid to match weather data
    electricity_df = electricity_df.withColumnRenamed("PriceArea", "Egrid")

    # Join weather and electricity data on hourly timestamps and price area (Egrid)
    combined_df = electricity_df.join(weather_features, ["hourly_observed", "Egrid"], "inner")

    # Select relevant features and target variable
    feature_columns = ["humidity", "temp_dry", "wind_speed", "cloud_cover"]  # Example weather variables
    combined_df = combined_df.select(feature_columns + ["SpotPriceDKK"]).dropna()

    # Assemble features into a single vector
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    feature_df = assembler.transform(combined_df).select("features", "SpotPriceDKK")

    # Split data into training and testing sets
    train_df, test_df = feature_df.randomSplit([0.8, 0.2], seed=42)

    # Train a linear regression model with hyperparameter tuning
    lr = LinearRegression(featuresCol="features", labelCol="SpotPriceDKK")

    # Create a parameter grid for tuning
    param_grid = ParamGridBuilder() \
        .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
        .build()

    # Set up cross-validation
    evaluator = RegressionEvaluator(labelCol="SpotPriceDKK", predictionCol="prediction", metricName="rmse")
    crossval = CrossValidator(estimator=lr, \
                              estimatorParamMaps=param_grid, \
                              evaluator=evaluator, \
                              numFolds=5)

    # Train the model using cross-validation
    cv_model = crossval.fit(train_df)

    # Get the best model from cross-validation
    best_model = cv_model.bestModel

    # Evaluate the best model on the testing set
    predictions = best_model.transform(test_df)
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) after tuning: {rmse}")

    # Calculate RMSE as a percentage of price range
    min_price = combined_df.agg({"SpotPriceDKK": "min"}).collect()[0][0]
    max_price = combined_df.agg({"SpotPriceDKK": "max"}).collect()[0][0]
    price_range = max_price - min_price
    rmse_percentage = (rmse / price_range) * 100
    print(f"RMSE as a percentage of price range after tuning: {rmse_percentage:.2f}%")

    # Save the best model to HDFS
    best_model.write().overwrite().save(model_hdfs_path)
    print(f"Best model saved to {model_hdfs_path}")

except Exception as e:
    print(f"Error: {e}")

finally:
    spark.stop()