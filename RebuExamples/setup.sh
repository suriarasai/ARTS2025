#!/bin/bash
# setup.sh - Project Setup Script

set -e

echo "🚕 Setting up Kafka PySpark Streaming Project..."

# Create project directory structure
echo "📁 Creating directory structure..."
mkdir -p kafka-pyspark-streaming/{data,src,output,checkpoints}
cd kafka-pyspark-streaming

# Create all the necessary files
echo "📝 Creating configuration files..."

# Create podman-compose.yml
cat > podman-compose.yml << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    hostname: kafka
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_JMX_PORT: 9094
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    volumes:
      - kafka-data:/var/lib/kafka/data

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181

  pyspark-app:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: pyspark-streaming
    depends_on:
      - kafka
    volumes:
      - ./data:/app/data
      - ./src:/app/src
      - ./output:/app/output
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
    command: tail -f /dev/null
    networks:
      - default

volumes:
  zookeeper-data:
  zookeeper-logs:
  kafka-data:

networks:
  default:
    driver: bridge
EOF

# Create Dockerfile
cat > Dockerfile << 'EOF'
FROM python:3.9-slim

# Install Java (required for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

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
EOF

# Create requirements.txt
cat > requirements.txt << 'EOF'
pyspark==3.4.1
kafka-python==2.0.2
pandas==2.1.0
numpy==1.24.3
python-dateutil==2.8.2
pytz==2023.3
EOF

echo "✅ Project structure created successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Copy your RebuTripData.csv file to: ./data/RebuTripData.csv"
echo "2. Run: podman-compose up -d"
echo "3. Wait for services to start (~30 seconds)"
echo "4. Run the streaming pipeline as described in README.md"
echo ""
echo "🌐 Services will be available at:"
echo "   • Kafka UI: http://localhost:8080"
echo "   • Kafka Bootstrap: localhost:9092"
echo ""
echo "🚕 Happy Streaming!"