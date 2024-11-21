
FROM python:3
RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY src/ .
COPY weather-data-urls-avro-producer.py .
CMD ["python", "weather-data-urls-avro-producer.py"]