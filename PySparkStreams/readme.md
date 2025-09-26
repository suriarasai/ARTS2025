# PySparkStreams

A collection of small, focused PySpark Structured Streaming examples that read from Kafka, files, and sockets. A Podman Compose stack is provided to run Kafka (with Zookeeper) and Kafka UI locally.


## Contents
- Overview and prerequisites
- File structure
- Start/stop the Podman stack
- Running examples on Windows (host Python)
- Running selected examples inside the container (Podman)
- Script-by-script guide (what each file does and how to run)
- Producing sample data to Kafka topics
- Troubleshooting notes


## Prerequisites
- Windows with:
  - Python 3.9+
  - Java 11 (required by PySpark)
  - Podman Desktop or Podman for Windows (with a running podman machine)
- Internet access (PySpark will download Kafka connector packages at runtime)

Optional utilities:
- Netcat/Ncat for socket demos, or use the Python alternatives listed below.

Install Python dependencies (host):

```bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Set Java for the current CMD session if needed:

```bat
set JAVA_HOME=C:\Program Files\Java\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%
```


## File structure
```
PySparkStreams/
├─ compose.yml
├─ Dockerfile
├─ requirements.txt
├─ run.sh
├─ setup.sh
├─ data/
│  ├─ booking/
│  │  ├─ Booking1.json ... Booking10.json
│  ├─ onefilebooking/Booking.json
│  ├─ rebu/
│  │  ├─ Booking.json
│  │  ├─ Customer.json
│  │  ├─ Taxi.json
│  │  ├─ booking/booking1.csv ... booking7.csv
│  │  └─ driver/Driver-Data-Set001.json ... 003.json
│  └─ window/
│     ├─ device-data.txt
│     ├─ sensors.txt
│     └─ trades.json
├─ output/ (created by some examples)
└─ src/
   ├─ BookingKafkaStreamProducer.py
   ├─ BookingSlidingWindow.py
   ├─ BookingTumblingWindow.py
   ├─ FileBookingStreamingDemo.py
   ├─ FileDriverStreamingDemo.py
   ├─ main.py
   ├─ ReadJSON.py
   ├─ SimpleBookingGenerator.py
   ├─ SimpleBookingProcessor.py
   ├─ SlidingWindowDemo.py
   ├─ StreamCompleteMode.py
   ├─ StreamUpdateMode.py
   ├─ StreamWordCount.py
   ├─ TriggersDemo.py
   ├─ TumblingWindowDemo.py
   ├─ WatermarkDemo.py
   └─ WindowAggregation.py
```


## Start/stop the local stack (Podman Compose)
This brings up Zookeeper, Kafka, Kafka UI, and an idle PySpark container.

Start:
```bat
podman compose -f compose.yml up -d
```

Stop and remove:
```bat
podman compose -f compose.yml down
```

Kafka UI will be available at:
- http://localhost:8080

Kafka listeners (from compose):
- Inside containers: kafka:29092
- From host: localhost:9092

Auto-create topics is enabled in the Kafka service, so consumers/producers can create topics on first use.


## Running examples on Windows (host Python)
This is the simplest way to run Kafka-based scripts in this repo because many scripts use kafka bootstrap "localhost:9092" (which works from the host but not from inside the container without editing code).

General pattern from the repo root:
```bat
python src\<ScriptName>.py
```

File-streaming demos expect the working directory to be the data folder. For those, run from data/ so relative paths resolve:
```bat
cd data
python ..\src\FileBookingStreamingDemo.py
```


## Running selected examples inside the container (Podman)
The PySpark container mounts the repo at /app. For file-based demos (no Kafka), you can exec into the container and run them. Use the data folder as working directory so relative file paths resolve.

Examples:
```bat
podman exec -w /app/data -it pyspark-streaming python /app/src/FileBookingStreamingDemo.py
podman exec -w /app/data -it pyspark-streaming python /app/src/FileDriverStreamingDemo.py
```

Important:
- Scripts that talk to Kafka are hard-coded to use "localhost:9092". Inside a container, "localhost" refers to the container itself, not the Kafka broker. Either run those scripts on the host or edit them to use "kafka:29092" when running inside the container.


## Script-by-script guide
Brief description and how to run each script. Unless stated otherwise, run from the repo root on the host.

- src\main.py
  - Tiny example that prints a greeting.
  - Run:
    ```bat
    python src\main.py
    ```

- src\ReadJSON.py
  - Reads newline-delimited JSON from data\onefilebooking\Booking.json and prints objects.
  - Run from data/ so the relative path resolves:
    ```bat
    cd data
    python ..\src\ReadJSON.py
    ```

- src\FileBookingStreamingDemo.py
  - Structured Streaming from CSV files in data\rebu\booking\ to CSV sink booking-output/ with checkpointing. One file per trigger.
  - Run (host):
    ```bat
    cd data
    python ..\src\FileBookingStreamingDemo.py
    ```
  - Run (container):
    ```bat
    podman exec -w /app/data -it pyspark-streaming python /app/src/FileBookingStreamingDemo.py
    ```

- src\FileDriverStreamingDemo.py
  - Structured Streaming from JSON files in data\rebu\driver\ to JSON sink driver-output/ with checkpointing.
  - Run (host):
    ```bat
    cd data
    python ..\src\FileDriverStreamingDemo.py
    ```
  - Run (container):
    ```bat
    podman exec -w /app/data -it pyspark-streaming python /app/src/FileDriverStreamingDemo.py
    ```

- src\BookingKafkaStreamProducer.py
  - Kafka producer that reads data\onefilebooking\Booking.json and sends each line to topic "booking".
  - Requires Kafka at localhost:9092.
  - Run (host):
    ```bat
    cd data
    python ..\src\BookingKafkaStreamProducer.py
    ```

- src\BookingTumblingWindow.py
  - Kafka consumer/processor that reads topic "booking" and performs a 10-minute tumbling window count on MessageSubmittedTime.
  - Requires messages like those produced by BookingKafkaStreamProducer.py.
  - Run (host):
    ```bat
    python src\BookingTumblingWindow.py
    ```

- src\BookingSlidingWindow.py
  - Kafka consumer/processor that reads topic "booking" and performs a 100-minute window with 10-minute slide (sliding window) counts.
  - Run (host):
    ```bat
    python src\BookingSlidingWindow.py
    ```

- src\SimpleBookingGenerator.py
  - Synthetic Kafka producer to topic "taxi_bookings" emitting simple JSON bookings every second.
  - Run (host):
    ```bat
    python src\SimpleBookingGenerator.py
    ```

- src\SimpleBookingProcessor.py
  - Kafka consumer/processor for topic "taxi_bookings". Parses JSON, converts timestamps, aggregates counts by pickup location per 1-minute window and prints to console.
  - Run (host):
    ```bat
    python src\SimpleBookingProcessor.py
    ```

- src\SlidingWindowDemo.py
  - Kafka consumer for topic "sensor". Demonstrates watermark + sliding windows, reports max Reading per SensorID over 15-minute window sliding every 5 minutes.
  - Run (host):
    ```bat
    python src\SlidingWindowDemo.py
    ```

- src\TumblingWindowDemo.py
  - Kafka consumer for topic "trades". Groups by 15-minute tumbling windows and sums Buy/Sell amounts.
  - Run (host):
    ```bat
    python src\TumblingWindowDemo.py
    ```

- src\WatermarkDemo.py
  - Kafka consumer for topic "trades" with watermarking (30 minutes), windowed aggregation, console output.
  - Run (host):
    ```bat
    python src\WatermarkDemo.py
    ```

- src\WindowAggregation.py
  - Kafka consumer for a generic topic (default "my-topic"). Demonstrates sliding windows (60s window, 30s slide) with watermark.
  - Update the topic in the script or send data to "my-topic". Run (host):
    ```bat
    python src\WindowAggregation.py
    ```

- src\TriggersDemo.py
  - Kafka consumer for topic "device-data" showing different trigger modes and schema flattening. Writes to console and memory.
  - Run (host):
    ```bat
    python src\TriggersDemo.py
    ```

- src\StreamWordCount.py
  - Socket streaming word count on port 9998 in complete mode with a 3s trigger.
  - Start a text source on port 9998, e.g. with ncat:
    ```bat
    ncat -l -p 9998
    ```
  - Then run:
    ```bat
    python src\StreamWordCount.py
    ```

- src\StreamCompleteMode.py
  - Socket streaming word count on port 9999 in complete mode.
  - Start a text source on port 9999, then run:
    ```bat
    python src\StreamCompleteMode.py
    ```

- src\StreamUpdateMode.py
  - Socket streaming word count on port 9999 in update mode.
  - Start a text source on port 9999, then run:
    ```bat
    python src\StreamUpdateMode.py
    ```


## Producing sample data to Kafka topics
You can use the included producers or Kafka CLI from inside the Kafka container. Kafka UI is also useful for producing messages.

Kafka console producer via container (examples):

- Sensor topic:
```bat
podman exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sensor
```
Example payloads (paste into the producer; the consumer expects JSON values like this and separate key):
```
{"CreatedTime":"2025-01-01 10:00:00","Reading":21.5}
{"CreatedTime":"2025-01-01 10:03:00","Reading":22.1}
```

- Trades topic:
```bat
podman exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic trades
```
Example lines:
```
{"CreatedTime":"2025-01-01 10:00:00","Type":"BUY","Amount":100,"BrokerCode":"BRK1"}
{"CreatedTime":"2025-01-01 10:01:30","Type":"SELL","Amount":70,"BrokerCode":"BRK2"}
```

- Device data topic:
```bat
podman exec -it kafka kafka-console-producer --bootstrap-server kafka:29092 --topic device-data
```
Example lines (simplified):
```
{"customerId":"c1","data":{"devices":[{"deviceId":"d1","measure":"temp","status":"OK","temperature":30}]},"eventId":"e1","eventOffset":1,"eventPublisher":"p","eventTime":"2025-01-01T10:00:00Z"}
```

- Booking topics using the provided producers (host):
```bat
cd data
python ..\src\BookingKafkaStreamProducer.py   REM sends to topic "booking"
python ..\src\SimpleBookingGenerator.py       REM sends to topic "taxi_bookings"
```


## Troubleshooting notes
- Inside-container Kafka access: change bootstrap servers to "kafka:29092" if you want to run Kafka-based scripts inside the container. As written, most scripts use "localhost:9092" and should be run on the host.
- Connector package versions: scripts pin different spark-sql-kafka versions (3.1.2, 3.3.0, 3.4.3). With PySpark 3.4.1 this usually works, but ensure internet access so Spark can download the jars. If you see version errors, align the package version with your PySpark version.
- File path expectations: file-streaming scripts use relative paths like "rebu\\booking" and expect the working directory to be the data folder. Run `cd data` first as shown above.
- Permissions and ports: ensure ports 9092 (Kafka), 2181 (Zookeeper), 8080 (Kafka UI), and 9998/9999 (socket demos) are free.
- Windows socket demos: if `ncat` is unavailable, you can use Python to send lines:
  ```bat
  python - <<PY
  import socket, time
  s=socket.socket(); s.connect(("127.0.0.1", 9999))
  for i in range(100):
      s.sendall(f"hello {i}\n".encode()); time.sleep(0.5)
  s.close()
  PY
  ```
- Output locations: many examples write to console; file demos write under the current working directory (e.g., booking-output/, driver-output/). Checkpoint directories are created automatically.


## Notes about the container image
The Dockerfile installs OpenJDK 11 and Python, then installs requirements. The compose service `pyspark-app` keeps the container running idle (tail -f /dev/null) so you can exec into it to run scripts. Source and data folders are bind-mounted into the container for fast edit-run cycles.

If you run into issues with the image or need to rebuild:
```bat
podman compose -f compose.yml build pyspark-app
podman compose -f compose.yml up -d --force-recreate
```

