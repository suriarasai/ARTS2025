# Welcome to Kafka CLI Tutorial
## Introduction
This example will guide you through setting up Kafka, creating a topic, sending messages with a producer, and reading them with a consumer, all from your terminal.
## Prerequisites
For testing this example, you will need:
- Podman and Podman Compose installed on your machine.
- Basic knowledge of terminal commands.
## Step 1: Clone the Repository
First, clone the repository to your local machine:

```bash
git clone https://github.com/suriarasai/ARTS2025
cd ARTS2025/SimplePodmanKafka
```

## Step 1a: Understand the Compose File
Before proceeding, take a moment to review the `compose.yml` file in this project folder. This file defines the services for Kafka and KRAFT, including their configurations and dependencies.

Contents of `compose.yml`:
```yaml
version: '3.8'
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka-kraft
    ports:
      - "9092:9092"
    environment:
      # KRaft settings
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka-kraft:9093'
      
      # Listeners
      KAFKA_LISTENERS: 'PLAINTEXT://:29092,CONTROLLER://:9093,EXTERNAL://:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka-kraft:29092,EXTERNAL://localhost:9092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT'
      
      # Storage and Cluster ID (Leave CLUSTER_ID commented out initially)
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # KAFKA_CLUSTER_ID: 'YourGeneratedClusterID'
 ```     
      
## Step 1b: Generate a Cluster ID
Before starting the Kafka service, you need to generate a unique Cluster ID. You can do this
using the Kafka command-line tool. Run the following command:

```bash
podman run --rm confluentinc/cp-kafka:7.6.1 kafka-storage.sh random-uuid
``` 
It will output a unique ID. Copy this ID. It will look something like this: A_Random_ID-like-this-12345.

## Step 2: Uodate and Launch the Cluster
Now, go back to your compose.yml file:

Uncomment the KAFKA_CLUSTER_ID line.

Paste the unique ID you just generated as its value.

Your file should now look like this (with your own ID):

```yaml
      KAFKA_CLUSTER_ID: 'A_Random_ID-like-this-12345'
```
Now, you can launch the Kafka cluster using Podman Compose:

```bash
podman compose -f compose.yml up -d
```

## Step 3: Create a Kafka Topic
Once the Kafka service is running, you can create a topic. Use the following command to create a topic named `my_topic`:    

```bash
podman exec -it kafka-kraft kafka-topics.sh --create --topic my_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
You can verify that the topic was created successfully by listing all topics:

```bash
podman exec -it kafka-kraft kafka-topics.sh --list --bootstrap-server localhost:9092
``` 
You should see `my_topic` in the list.

## Step 4: Produce Messages to the Topic
Now, you can send messages to the topic using the Kafka console producer. Run the following command
```bash
podman exec -it kafka-kraft kafka-console-producer.sh --topic my_topic --bootstrap-server localhost:9092
```
Type a few messages and press Enter after each one. For example:
```
Hello, Kafka!
This is a test message used in ARTS course.
Kafka is fun!
But I prefer RabbitMQ.
Thank you!
```
To exit the producer, press `Ctrl+C`.
## Step 5: Consume Messages from the Topic
Now, you can read the messages you just sent using the Kafka console consumer. Run the following command:
```bash
podman exec -it kafka-kraft kafka-console-consumer.sh --topic my_topic --bootstrap-server localhost:9092 --from-beginning
```
You should see the messages you typed earlier displayed in the terminal.    

To exit the consumer, press `Ctrl+C`.
## Step 6: Clean Up
When you are done testing, you can stop and remove the Kafka service using Podman Compose:
```bash
podman compose -f compose.yml down
```
## Conclusion
You have successfully set up a Kafka cluster using Podman, created a topic, produced messages,
and consumed them, all from the terminal. This setup is great for learning and testing Kafka in a local environment. For production use, consider additional configurations for security, scalability, and reliability.
## Additional Resources
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Podman Documentation](https://podman.io/getting-started/)
  - [Confluent Kafka Docker Images](https://hub.docker.com/r/confluentinc/cp-kafka)
  Feel free to explore and experiment with Kafka further!
  - [Kafka KRaft Mode](https://www.confluent.io/blog/kraft-the-new-kafka-native-replication-and-transaction-log/)
  - [Kafka CLI Tools](https://kafka.apache.org/documentation/#basic_ops_cli)
  - [Kafka Topics](https://kafka.apache.org/documentation/#topicconfigs)
  - [Kafka Producers and Consumers](https://kafka.apache.org/documentation/#producerapi)
    - [Kafka Configuration](https://kafka.apache.org/documentation/#configuration)  
    - [Kafka Security](https://kafka.apache.org/documentation/#security)
    - [Kafka Monitoring](https://kafka.apache.org/documentation/#monitoring)
    - [Kafka Connect](https://kafka.apache.org/documentation/#connect)
    - [Kafka Streams](https://kafka.apache.org/documentation/#streams)
    - [Kafka Admin Client](https://kafka.apache.org/documentation/#adminclient)
    - [Kafka Performance Tuning](https://kafka.apache.org/documentation/#performance)
    - [Kafka Best Practices](https://kafka.apache.org/documentation/#best_pract
    - [Kafka Use Cases](https://kafka.apache.org/documentation/#use_cases)
    - [Kafka Community](https://kafka.apache.org/community)
    - [Kafka Tutorials](https://kafka.apache.org/quickstart)
