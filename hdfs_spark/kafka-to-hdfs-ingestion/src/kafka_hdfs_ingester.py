from kafka import KafkaConsumer
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests, zipfile, io
import logging

KAFKA_TOPIC: str = 'ENERGY_DATA'
KAFKA_BROKER: str = 'kafka:9092'
HDFS_PATH: str = 'hdfs://namenode:9000/'
DEFAULT_ENCODING: str = "utf-8"

def consumeMessages(logger):
    logger.debug(f"Consumer af:\nTOPIC: {KAFKA_TOPIC}\nBootstrap Server:{KAFKA_BROKER}")
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER)
    for msg in consumer:
        logger.debug(f"Message: {msg} in consumer")
        yield msg.value.decode(DEFAULT_ENCODING)

def get_historical_weather_data():
    api_key = '50b0870c-6f45-4dab-8269-43890d141299'
    base_url = 'https://dmigw.govcloud.dk/v2/metObs/bulk/'

    """
        data_type:
        Specifices which historical data to pull.
        Can be all.zip, 2022/2022.zip or 2011/2011.zip etc.
        Refferance: https://opendatadocs.dmi.govcloud.dk/en/Download
    """
    data_type = '2022/2022.zip'

    r = requests.get(f'{base_url}{data_type}?api-key={api_key}', stream=True)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("/app/historical_data")


def writeToHDFS(data, logger):
    """
        Write the data in a parquet format.
        The written Paruqet file will be in hdfs://hdfs_server:port/path/output.parquet
    """
    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df)
    logger.debug(f"Writing data:\n{data}\nto hdfs path:\n{HDFS_PATH}")
    pq.write_table(table,HDFS_PATH,"output.parquet",append=True)

def main():
    logging.basicConfig(filename="logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
    
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    data = []
    while True:
        for record in consumeMessages(logger):
            data.append(record)
            logger.debug(f"Record {record} is added to the array")
            if (len(data)>100):
                writeToHDFS(data, logger)
                logger.debug("Data are written to hdfs")
                data.clear()

if __name__ == "__main__":
    main()
