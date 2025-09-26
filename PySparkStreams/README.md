# PySpark Structured Streaming — Windows (PowerShell) + PyCharm

This repository contains four small PySpark streaming examples you can run on a Windows machine using **PyCharm** and **PowerShell**. Two examples read from a **TCP socket** (type text into a terminal and see live word counts), and one example uses **Kafka** for windowed aggregation.

---

## Contents (scripts)

- `StreamCompleteMode.py` — Socket word count with `outputMode("complete")`.   
- `StreamUpdateMode.py` — Socket word count with `outputMode("update")`.   
- `StreamWordCount.py` — Socket word count on **port 9998** with a 3‑second trigger. 
- `WindowAggregation.py` — Kafka sliding window aggregation over JSON messages.  

---

## Prerequisites

### 1) Python & Java
- **Python 3.9–3.13** recommended (PySpark supports specific versions; 3.12+ may not work with some Spark releases).
- **Java 8 or 11 (JDK)**: Install Temurin or Oracle JDK and ensure `JAVA_HOME` and `PATH` are set.

### 2) PySpark (no local Spark install required for basic runs)
In an isolated virtual environment:
```powershell
# In PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install pyspark
```
> If you plan to run the **Kafka** example, also install the matching **Spark Kafka** package at runtime (see below) or add the correct package via `spark.jars.packages` (already present in `WindowAggregation.py`).

### 3) Optional tools for the socket source
To **type data** into a TCP socket locally you need a small TCP server (listener). On Windows any of these options work:

- **Ncat (recommended)**: install [Nmap](https://nmap.org/ncat/) which includes `ncat`.
  ```powershell
  # listen on 9999 (examples 1 & 2)
  ncat -lk 9999

  # listen on 9998 (example 3)
  ncat -lk 9998
  ```

- **Git Bash** (ships with `nc` on many installs):
  ```bash
  nc -lk 9999
  nc -lk 9998
  ```

- **Pure PowerShell (.NET TCP listener)** — if you cannot install `ncat`/`nc`, you can spin up a tiny listener with this one‑liner (press `Ctrl+C` to stop):
  ```powershell
  pwsh -NoLogo -NoProfile -Command "
    $l = [System.Net.Sockets.TcpListener]::new([Net.IPAddress]::Any, $env:PORT);
    $l.Start();
    Write-Host 'Listening on' $env:PORT;
    $c = $l.AcceptTcpClient();
    $s = $c.GetStream();
    $w = New-Object IO.StreamWriter($s);
    while($true){ $line = Read-Host; $w.WriteLine($line); $w.Flush() }" 
  ```
  Run it for each port by prefixing `PORT=9999` (or `9998`):
  ```powershell
  $env:PORT=9999; <paste the one-liner above>
  ```

> The PySpark scripts connect **as clients** to these listeners. Whatever you type in the listener console becomes the streaming input.

### 4) (Optional) Kafka (for the windowed aggregation example)
You need a Kafka broker and a topic (e.g., `my-topic`). The simplest way on Windows is using **Docker Desktop** or **Podman Desktop**.

**Quick start with Docker (single-broker dev setup):**
```powershell
# Start Kafka (includes Zookeeper, for dev)
docker run -d --name zookeeper -p 2181:2181 zookeeper:3.9

docker run -d --name kafka -p 9092:9092 `
  -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 `
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 `
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 `
  confluentinc/cp-kafka:7.6.1

# Create topic
docker exec -it kafka kafka-topics --create --topic my-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Send test messages (each line a JSON object)
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic my-topic
# paste lines like: {"event_time":"2025-09-26T14:25:00","value":"alpha"}
```

---

## Open in PyCharm

1. **Clone / open** this folder in PyCharm.
2. **Create a Python Interpreter**: *File → Settings → Project → Python Interpreter* → add existing `.\.venv` or create a new one and install `pyspark`.
3. (Optional) **Run configurations**: For each script, create a separate run/debug configuration pointing to the corresponding `.py` file.

---

## How to run each example

> In all socket examples: **start the TCP listener first**, then run the PySpark script from PyCharm, then type words in the listener window.

### A) `StreamCompleteMode.py`
- **What it does:** classic streaming **word count**; prints a full table snapshot every trigger using `outputMode("complete")`. fileciteturn0file0
- **Start a listener (9999):**
  ```powershell
  ncat -lk 9999    # or use the PowerShell listener snippet
  ```
- **Run in PyCharm:** right‑click `StreamCompleteMode.py` → *Run*.
- **Test:** type sentences into the listener window; the console running PySpark prints an updated **complete** counts table.

### B) `StreamUpdateMode.py`
- **What it does:** same as above but uses `outputMode("update")` (only changed rows are printed). fileciteturn0file1
- **Listener (9999):**
  ```powershell
  ncat -lk 9999
  ```
- **Run in PyCharm** and type input. You’ll see only updated word rows.

### C) `StreamWordCount.py`
- **What it does:** word count on **port 9998** with a **3‑second trigger** (`trigger(processingTime='3 seconds')`). fileciteturn0file2
- **Listener (9998):**
  ```powershell
  ncat -lk 9998
  ```
- **Run in PyCharm** and type input; output prints every ~3 seconds.

### D) `WindowAggregation.py` (Kafka sliding window)
- **What it does:** reads JSON messages from Kafka, parses fields `event_time` (timestamp) and `value` (string), then performs a **sliding window** aggregation with watermarking and prints results using `outputMode("update")`. fileciteturn0file3
- **Pre-run checklist:**
  - Kafka is running and reachable at `localhost:9092`.
  - Topic (default in script): `my-topic` exists (create with the commands above or change in the script).
- **Run in PyCharm:** just run the script; package coordinates for the Kafka connector are specified in the script via
  ```python
  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
  ```
- **Produce messages:** In a separate terminal:
  ```powershell
  # If using the dockerized Kafka from above
  docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic my-topic
  # paste JSON lines like:
  {"event_time":"2025-09-26T14:26:00","value":"alpha"}
  {"event_time":"2025-09-26T14:26:15","value":"beta"}
  {"event_time":"2025-09-26T14:26:25","value":"alpha"}
  ```
- **Observe:** the PySpark console prints rolling counts per sliding window.

---

## Troubleshooting (Windows)

- **Java errors (`JAVA_HOME` not set):** install JDK 8/11 and set environment variables, then restart PyCharm.
- **Port already in use:** change the port in both the listener and the script (e.g., 9997).
- **Firewall prompt:** allow local connections for `python.exe` and your terminal app.
- **Kafka connector mismatch:** ensure the Kafka package version (`spark-sql-kafka-0-10_2.12:<spark_version>`) matches your Spark major/minor version. The script uses `3.3.0` as a safe baseline.
- **Unicode characters not showing:** switch font/encoding in the terminal or type ASCII for testing.

---

## What to type to test quickly

- For socket examples: just type phrases like
  ```
  hello from spark
  hello hello
  streaming is fun
  ```
- For Kafka example: paste JSON with ISO timestamps:
  ```json
  {"event_time":"2025-09-26T14:30:00","value":"alpha"}
  {"event_time":"2025-09-26T14:30:05","value":"alpha"}
  {"event_time":"2025-09-26T14:30:10","value":"beta"}
  ```

---

## Notes

- You can run multiple socket examples in parallel if you use **different ports** and separate listener terminals.
- For teaching/demos, keep the PyCharm *Run* toolwindow docked to watch live console output while you type into the listener terminal.
