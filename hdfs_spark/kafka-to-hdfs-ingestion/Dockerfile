FROM python:3
RUN mkdir /app
RUN mkdir /app/historical_data
WORKDIR /app
COPY ../requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "kafka_hdfs_ingester.py"]
COPY src/ . 