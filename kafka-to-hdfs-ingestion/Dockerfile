FROM python:3
WORKDIR /app
COPY ../requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "kafka_hdfs_ingester.py"]
COPY src/ . 