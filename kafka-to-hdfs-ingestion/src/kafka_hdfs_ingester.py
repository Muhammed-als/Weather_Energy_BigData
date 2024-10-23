from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

KAFKA_TOPIC: str = 'topic_name'
KAFKA_BROKER: str = 'kafka_broker:9092'
HDFS_PATH: str = 'hdfs://hdfs_server:port/path/'
DEFAULT_ENCODING: str = "utf-8"
def consumeMessages():
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER)
    for msg in consumer:
        yield msg.value.decode(DEFAULT_ENCODING)
def writeToHDFS(data):
    """Write the data in a parquet format. \n
        The written Paruqet file will be in hdfs://hdfs_server:port/path/output.parquet
    """
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    pq.write_table(table,HDFS_PATH,"output.parquet",append=True)
def main():
    data = []
    for record in consumeMessages():
        data.append(record)
        # Instead of write each record alone, we write for every 100 records, which reduce the amount of I/O operatons. 
        if len(data) > 100: 
            writeToHDFS(data)
            data.clear()  # Clear data after writing

if __name__ == "__main__":
    main()
