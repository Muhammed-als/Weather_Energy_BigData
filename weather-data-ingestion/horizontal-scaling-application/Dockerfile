
FROM python:3
RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
COPY .py .
CMD ["python", ".py"]