# Rebu 🚕 Real-Time Taxi Analytics with Kafka & PySpark

A comprehensive streaming data pipeline demonstrating **Kafka**, **PySpark Structured Streaming**, **windowing operations**, and **watermarking** concepts using Singapore taxi trip data.

## Overview

This project showcases:
- **Real-time data streaming** with Kafka producers
- **Tumbling vs Sliding Windows** in PySpark Structured Streaming
- **Watermarking strategies** for handling late-arriving data
- **Live taxi analytics** with multiple aggregation patterns

## Pipeline Architecture

```
CSV Data → Kafka Producer → Kafka Topic → PySpark Consumers
                                       ├── Tumbling Windows
                                       ├── Sliding Windows
                                       └── Watermarking Demo
```

## Project Folder Structure

```
kafka-pyspark-streaming/
├── podman-compose.yml          # Infrastructure setup
├── Dockerfile                  # PySpark application container
├── requirements.txt            # Python dependencies
├── README.md                   # This file
├── data/
│   └── RebuTripData.csv       # Singapore taxi trip data
├── src/
│   ├── taxi_data_producer.py  # Kafka producer
│   ├── tumbling_window_consumer.py  # Tumbling window demo
│   ├── sliding_window_consumer.py   # Sliding window demo
│   └── watermarking_demo.py   # Watermarking concepts
└── output/                     # Query outputs (auto-created)
```

## Quick Start

### 1. Prerequisites
- **Podman** and **podman-compose** installed
- **8GB+ RAM** recommended
- **CSV data file** placed in `data/RebuTripData.csv`

### 2. Setup Infrastructure
```bash
# Start Kafka, Zookeeper, and Kafka UI
podman compose up -d

# Check services are running
podman compose ps
```

### 3. Access Kafka UI
- Open http://localhost:8080 in your browser
- Monitor topics, messages, and consumer groups

### 4. Run the Streaming Pipeline

#### Start Data Producer
```bash
# Enter the PySpark container
podman exec -it pyspark-streaming bash

# Start streaming taxi data (10 events/sec for 30 minutes)
python src/taxi_data_producer.py 10 30
```

#### Run Tumbling Window Analysis
```bash
# In another terminal
podman exec -it pyspark-streaming bash
python src/tumbling_window_consumer.py
```

#### Run Sliding Window Analysis
```bash
# In another terminal  
podman exec -it pyspark-streaming bash
python src/sliding_window_consumer.py
```

#### Run Watermarking Demo
```bash
# In another terminal
podman exec -it pyspark-streaming bash
python src/watermarking_demo.py
```

##  Window Operations Explained

###  Tumbling Windows
- **Non-overlapping** time windows
- Each event belongs to exactly **one window**
- Good for **distinct time periods** (hourly reports, daily summaries)

**Example**: 2-minute tumbling windows
```
00:00-00:02 | 00:02-00:04 | 00:04-00:06 | ...
```

###  Sliding Windows
- **Overlapping** time windows
- Events can belong to **multiple windows**
- Good for **trend analysis** and **smoothing**

**Example**: 5-minute sliding windows with 1-minute slide
```
00:00-00:05
    00:01-00:06
        00:02-00:07
            00:03-00:08 | ...
```

### Key Differences

| Aspect | Tumbling Windows | Sliding Windows |
|--------|------------------|-----------------|
| **Overlap** | No | Yes |
| **Memory Usage** | Lower | Higher |
| **Use Case** | Distinct periods | Trend analysis |
| **Update Frequency** | Once per window | Every slide interval |
| **Computation** | Less | More |

## 💧 Watermarking Deep Dive

**Watermarking** handles **out-of-order** and **late-arriving** data in streaming systems.

### How It Works
1. **Watermark** = Latest event time seen - watermark delay
2. Windows older than watermark get **finalized** and **output**
3. Late data beyond watermark gets **dropped**

### Watermark Strategies

####  Strict Watermark (30 seconds)
```python
.withWatermark("event_timestamp", "30 seconds")
```
- **Pros**: Low memory usage, fast processing
- **Cons**: May drop valid late data
- **Use case**: Real-time dashboards, low-latency alerts

####  Lenient Watermark (5 minutes)  
```python
.withWatermark("event_timestamp", "5 minutes")
```
- **Pros**: Captures more late data, higher accuracy
- **Cons**: Higher memory usage, slower processing
- **Use case**: Batch reports, accuracy-critical analytics

####  No Watermark
```python
# No watermark specified
```
- **Pros**: Never drops data, complete accuracy
- **Cons**: Memory grows indefinitely, will crash
- **Use case**: Testing only, never in production

### Choosing the Right Watermark

Consider these factors:

1. **Data Arrival Patterns**: How late does your data typically arrive?
2. **Memory Constraints**: How much memory can you allocate?
3. **Accuracy Requirements**: Can you tolerate some data loss?
4. **Latency Requirements**: How fast do you need results?

**Rule of thumb**: Set watermark to **95th percentile** of your data lateness.

##  Demo Analytics

### Tumbling Window Queries
- **2-minute windows**: Trip count, revenue, average fare
- **3-minute windows**: District popularity analysis
- **5-minute windows**: Taxi type performance metrics

### Sliding Window Queries
- **5-min window, 1-min slide**: Overall trip trends
- **4-min window, 30-sec slide**: District popularity trends
- **3-min window, 30-sec slide**: Real-time taxi performance
- **6-min window, 2-min slide**: Rush hour detection

### Watermarking Queries
- **Strict vs Lenient**: Compare data completeness
- **Late data analysis**: Categorize events by arrival lateness
- **Memory usage**: Observe resource consumption differences

## Configuration Options

### Producer Configuration
```bash
# Customize event rate and duration
python src/taxi_data_producer.py <events_per_second> <duration_minutes>

# Examples:
python src/taxi_data_producer.py 5 60    # 5 events/sec for 1 hour
python src/taxi_data_producer.py 50 10   # 50 events/sec for 10 minutes
```

### Kafka Topics
- **Primary topic**: `taxi-trips`
- **Partitions**: 3 (auto-created)
- **Replication**: 1

### Spark Configuration
```python
# Memory optimization
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Custom checkpointing
.option("checkpointLocation", "/app/checkpoints")
```

## Monitoring & Observability

### Kafka UI (http://localhost:8080)
- Monitor topic throughput
- View message contents
- Track consumer lag
- Observe partition distribution

### Spark Streaming UI
Access Spark UI at `http://localhost:4040` when queries are running:
- Query progress and metrics
- Batch processing times
- Memory usage
- Watermark progression

### Log Monitoring
```bash
# Follow producer logs
podman logs -f pyspark-streaming

# Monitor Kafka logs
podman logs -f kafka
```

## Troubleshooting

### Common Issues

#### Producer Not Sending Data
```bash
# Check Kafka connectivity
podman exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Verify CSV file exists
podman exec pyspark-streaming ls -la /app/data/
```

#### Consumer Not Receiving Data
```bash
# Check if topic exists and has data
podman exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic taxi-trips --from-beginning --max-messages 5
```

#### Out of Memory Errors
```bash
# Increase Spark memory (in Dockerfile)
ENV SPARK_DRIVER_MEMORY=2g
ENV SPARK_EXECUTOR_MEMORY=2g

# Or reduce window sizes and event rates
```

#### Slow Processing
- Reduce window sizes
- Increase trigger intervals
- Lower event production rate
- Add more Kafka partitions

### Resource Requirements

| Component | CPU | Memory | Storage |
|-----------|-----|---------|----------|
| Kafka | 1 core | 1GB | 5GB |
| Zookeeper | 0.5 core | 512MB | 1GB |
| PySpark | 2+ cores | 2+ GB | 1GB |
| **Total** | **3.5+ cores** | **3.5+ GB** | **7GB** |

## More Trials 

### Custom Window Functions
```python
# Custom aggregations
.agg(
    count("*").alias("total_events"),
    approx_count_distinct("trip_id").alias("unique_trips"),
    percentile_approx("fare_amount", 0.95).alias("p95_fare"),
    collect_list("pickup_district").alias("all_districts")
)
```

### Multiple Output Sinks
```python
# Write to multiple sinks
query = df.writeStream \
    .foreachBatch(lambda batch, epoch: {
        batch.write.mode("append").json(f"/app/output/epoch_{epoch}"),
        batch.write.mode("append").format("console").save()
    }) \
    .start()
```

### State Management
```python
# Stateful operations
from pyspark.sql.streaming import GroupState

def update_trip_state(key, values, state):
    # Custom stateful logic
    return updated_state

df.groupByKey(lambda x: x.pickup_district) \
  .mapGroupsWithState(update_trip_state, ...) \
  .writeStream.start()
```

## In Summary
After completing this workshop, you'll understand:

1. **Kafka Fundamentals**:
   - Producer/Consumer patterns
   - Topic partitioning
   - Message serialization

2. **PySpark Structured Streaming**:
   - DataFrame API for streaming
   - Trigger modes and output modes
   - Checkpointing and fault tolerance

3. **Window Operations**:
   - When to use tumbling vs sliding
   - Performance implications
   - Memory management

4. **Watermarking**:
   - Late data handling strategies
   - Trade-offs between accuracy and resources
   - Production considerations

5. **Stream Processing Patterns**:
   - Real-time aggregations
   - Event-time vs processing-time
   - Stateful computations



## Extend

1. Fork the repository
2. Create feature branches
3. Add tests for new functionality
4. Submit pull requests with clear descriptions

##  License

This project is open-source and available under the MIT License.

---

**Happy Streaming! with Rebu 🚕Taxis **
