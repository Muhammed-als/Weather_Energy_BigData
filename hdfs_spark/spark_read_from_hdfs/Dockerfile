FROM python:3.8-slim

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    libssl-dev \
    libffi-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip3 install --upgrade pip

# Set the working directory
WORKDIR /app

# Copy application files
COPY reader.py /app
COPY requirements.txt /app

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt
