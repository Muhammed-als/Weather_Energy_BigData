
FROM python:3
RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir /app/src
COPY C:/GitHubDesctopRepositories/Weather_Energy_BigData/weather-data-ingestion/src/ ./src
COPY C:/GitHubDesctopRepositories/Weather_Energy_BigData/weather-data-ingestion/weather-data-urls-avro-producer.py .
CMD ["python", "weather-data-urls-avro-producer.py"]