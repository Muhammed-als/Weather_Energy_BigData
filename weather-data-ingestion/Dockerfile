FROM python:3
RUN mkdir /app
RUN mkdir /app/weather-data-ingestion
WORKDIR /app
COPY ./requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "dmi_live.py"]
COPY ./src . 