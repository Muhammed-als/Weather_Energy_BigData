
FROM python:3
RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir /app/src
COPY src/ ./src
COPY autoscaler-script.py .
CMD ["python", "autoscaler-script.py"]