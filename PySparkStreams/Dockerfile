# Dockerfile for PySpark 3.5 LTS with ncat support
FROM openjdk:11-jdk-slim

# Set environment variables
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    netcat-openbsd \
    nmap \
    procps \
    net-tools \
    vim \
    htop \
    && rm -rf /var/lib/apt/lists/*

# Create symbolic link for python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Download and install Spark
RUN wget -q "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" -C /opt/ \
    && ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" "$SPARK_HOME" \
    && rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Kafka connector for Spark
RUN cd $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Set working directory
WORKDIR /app

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /app/src /app/data /app/output /app/checkpoints /app/logs /app/notebooks

# Create Spark configuration directory and default config
RUN mkdir -p $SPARK_HOME/conf
RUN echo "# Spark Defaults" > $SPARK_HOME/conf/spark-defaults.conf

# Create startup scripts using multiple echo commands
RUN echo "#!/bin/bash" > /app/start-spark.sh
RUN echo "echo 'Starting Spark services...'" >> /app/start-spark.sh
RUN echo "\$SPARK_HOME/sbin/start-master.sh" >> /app/start-spark.sh
RUN echo "\$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077" >> /app/start-spark.sh
RUN echo "tail -f /dev/null" >> /app/start-spark.sh
RUN chmod +x /app/start-spark.sh

# Create ncat listener script
RUN echo "#!/bin/bash" > /app/start-ncat.sh
RUN echo "echo 'Starting ncat listener on port 9999...'" >> /app/start-ncat.sh
RUN echo "nc -lk 9999" >> /app/start-ncat.sh
RUN chmod +x /app/start-ncat.sh

# Create main startup script
RUN echo "#!/bin/bash" > /app/startup.sh
RUN echo "echo '=== PySpark 3.5 LTS Container Started ==='" >> /app/startup.sh
RUN echo "echo 'Spark Home: \$SPARK_HOME'" >> /app/startup.sh
RUN echo "echo 'Python Path: \$PYTHONPATH'" >> /app/startup.sh
RUN echo "echo 'Java Home: \$JAVA_HOME'" >> /app/startup.sh
RUN echo "echo '========================================='" >> /app/startup.sh
RUN echo "" >> /app/startup.sh
RUN echo "# Start Spark services" >> /app/startup.sh
RUN echo "\$SPARK_HOME/sbin/start-master.sh" >> /app/startup.sh
RUN echo "sleep 5" >> /app/startup.sh
RUN echo "\$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077" >> /app/startup.sh
RUN echo "" >> /app/startup.sh
RUN echo "# Start Jupyter Notebook (optional)" >> /app/startup.sh
RUN echo "if [ \"\$START_JUPYTER\" = \"true\" ]; then" >> /app/startup.sh
RUN echo "    echo 'Starting Jupyter Notebook...'" >> /app/startup.sh
RUN echo "    jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=\"\" &" >> /app/startup.sh
RUN echo "fi" >> /app/startup.sh
RUN echo "" >> /app/startup.sh
RUN echo "echo '=== Services Started ==='" >> /app/startup.sh
RUN echo "echo 'Spark Master UI: http://localhost:4040'" >> /app/startup.sh
RUN echo "echo 'Spark Worker UI: http://localhost:8081'" >> /app/startup.sh
RUN echo "echo 'Jupyter Notebook: http://localhost:8888'" >> /app/startup.sh
RUN echo "echo 'ncat socket: nc localhost 9999'" >> /app/startup.sh
RUN echo "echo '====================='" >> /app/startup.sh
RUN echo "" >> /app/startup.sh
RUN echo "# Keep container running" >> /app/startup.sh
RUN echo "tail -f /dev/null" >> /app/startup.sh
RUN chmod +x /app/startup.sh

# Expose ports
EXPOSE 4040 7077 8081 8888 9999

# Set default command
CMD ["/app/startup.sh"]