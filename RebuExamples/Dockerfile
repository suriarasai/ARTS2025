# Dockerfile
FROM python:3.9-slim

# Dockerfile - Use official OpenJDK 11 base with Python
FROM openjdk:11-jdk-slim

# Install Python 3.9 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip procps curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Verify installations
RUN java -version && python --version

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/src /app/output /app/checkpoints

# Copy application files
COPY . .

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/local/openjdk-11

CMD ["bash"]

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/data /app/src /app/output /app/checkpoints

# Copy application files
COPY . .

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

CMD ["bash"]