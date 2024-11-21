from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("ParquetReader") \
    .getOrCreate()
HDFS_PATH: str = 'hdfs://hdfs_server:port/path/'
df = spark.read.parquet(HDFS_PATH)



