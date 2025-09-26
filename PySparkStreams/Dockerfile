FROM python:3.9-slim

# Install Java (required for Spark) and basic tools
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Try to install netcat (different package names on different systems)
RUN apt-get update && \
    (apt-get install -y netcat-openbsd || \
     apt-get install -y netcat-traditional || \
     apt-get install -y netcat) && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment (will be auto-detected)
ENV JAVA_HOME=/usr/lib/jvm/default-java

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