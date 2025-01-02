FROM python:3.9

# Set working directory
WORKDIR /app

# Copy the Kafka-ingestion directory contents
COPY ./Kafka-ingestion/ /app/

# Copy requirements.txt (it needs to be in the same directory as the Dockerfile)
COPY requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose any necessary ports (if needed)
# EXPOSE

# Run the main application
CMD ["python", "main.py"]