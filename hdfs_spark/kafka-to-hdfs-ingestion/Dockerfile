FROM python:3.11

RUN mkdir /app
RUN mkdir /app/historical_data

WORKDIR /app

COPY ../requirements.txt .
RUN pip install -r requirements.txt

COPY src/ .

CMD ["python", "kafka_hdfs_ingester.py"]