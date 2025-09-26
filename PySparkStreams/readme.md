# PySpark 3.5 LTS Streaming with Kafka and ncat

A complete development environment for Apache Spark 3.5 LTS with Kafka integration and socket streaming capabilities using ncat, optimized for Windows 11, PyCharm IDE, and Podman Desktop.

## Features

- **Apache Spark 3.5 LTS** with PySpark
- **Apache Kafka** with KRaft mode (no Zookeeper)
- **ncat/netcat** for socket streaming
- **Kafka UI** for monitoring and management
- **Jupyter Notebook** possible integration
- **Windows 11** optimized with Podman Desktop
- **PyCharm IDE** remote interpreter support
- **Automated development workflows** with make.bat

##  Prerequisites

- Windows 11
- [Podman Desktop](https://podman-desktop.io/) installed
- [PyCharm IDE](https://www.jetbrains.com/pycharm/) (Community or Professional)
- Git for Windows

## Quick Start

1. **Clone or create your project directory:**
   ```bash
   mkdir pyspark-streaming
   cd pyspark-streaming
   ```

2. **Copy all the provided files to your project directory**

3. **Build and start services:**
   ```cmd
   make.bat build
   make.bat up
   ```

4. **Verify everything is running:**
   ```cmd
   make.bat status
   make.bat health
   ```

## Available Commands

### Service Management
```cmd
make.bat up           # Start all services
make.bat down         # Stop all services
make.bat restart      # Restart all services
make.bat status       # Show container status
make.bat health       # Check service health
```

### Development
```cmd
make.bat shell        # Enter PySpark container
make.bat jupyter      # Start Jupyter notebook
make.bat pycharm      # PyCharm setup instructions
make.bat debug        # Start in debug mode
```

### Monitoring
```cmd
make.bat spark-ui     # Open Spark UI (http://localhost:4040)
make.bat kafka-ui     # Open Kafka UI (http://localhost:8080)
make.bat logs         # Show all logs
make.bat logs-app     # Show PySpark logs only
```

### Socket Streaming
```cmd
make.bat ncat         # Start ncat listener on port 9999
make.bat test-socket  # Test socket connection
```

### Kafka Operations
```cmd
make.bat topics           # List Kafka topics
make.bat create-topic     # Create a new topic
```

### Cleanup
```cmd
make.bat clean        # Clean containers and volumes
make.bat clean-all    # Clean everything including images
```

## PyCharm IDE Integration

1. **Start the services:**
   ```cmd
   make.bat up
   ```

2. **Configure Remote Interpreter:**
   - Open PyCharm Settings (File → Settings)
   - Navigate to Project → Python Interpreter
   - Click the gear icon → Add...
   - Select "Docker" or "Podman"
   - Configure:
     - **Server**: podman
     - **Image**: Use existing container `pyspark-streaming`
     - **Python interpreter path**: `/usr/bin/python3`

3. **Set up Run Configurations:**
   - Create new Python configuration
   - Set script path to your Python files in `/app/src`
   - Set environment variables if needed

## Socket Streaming Examples

### Basic Socket

1. **Start ncat listener:**
   ```cmd
   make.bat ncat
   ```

2. **Run the streaming example:**
   ```cmd
   make.bat shell
   cd /app/src
   python socket_streaming_example.py socket
   ```

3. **Send test data:**
   ```cmd
   echo "Hello Spark Streaming!" | nc localhost 9999
   ```

### Socket to Kafka Pipeline

1. **Create Kafka topic:**
   ```cmd
   make.bat create-topic
   # Enter: socket-messages
   ```

2. **Run the pipeline:**
   ```cmd
   make.bat shell
   python socket_streaming_example.py pipeline
   ```

3. **Send data and monitor:**
   ```cmd
   # Send data
   echo "Processing message 123" | nc localhost 9999
   
   # Monitor Kafka UI
   make.bat kafka-ui
   ```

## Debugging and Development

### Debug Mode
```cmd
make.bat debug
```
This starts services with remote debugging enabled on port 5005.

### View Logs
```cmd
# All services
make.bat logs

# Specific service
make.bat logs-app
make.bat logs-kafka
```

### Interactive Development
```cmd
# Enter container shell
make.bat shell

# Start Python/PySpark shell
pyspark

# Or start IPython
ipython
```

## Monitoring and UIs

| Service | URL | Description |
|---------|-----|-------------|
| Spark UI | http://localhost:4040 | Spark jobs and stages |
| Kafka UI | http://localhost:8080 | Kafka topics and messages |
| Jupyter | http://localhost:8888 | Notebook interface |

## 🗂️ Directory Structure

```
your-project/
├── compose.yml              # Docker compose configuration
├── Dockerfile              # PySpark container definition
├── requirements.txt         # Python dependencies
├── spark-defaults.conf      # Spark configuration
├── make.bat                # Windows automation script
├── README.md               # This file
├── src/                    # Your Python source code
├── data/                   # Input data files
├── output/                 # Output results
├── checkpoints/           # Spark streaming checkpoints
├── logs/                  # Application logs
└── notebooks/             # Jupyter notebooks
```

## Troubleshooting

### Container Won't Start
```cmd
make.bat logs-app
# Check for Java/Python path issues
```

### Kafka Connection Issues
```cmd
make.bat health
# Verify Kafka is healthy before starting Spark streaming
```

### Socket Connection Refused
```cmd
make.bat test-socket
# Ensure ncat listener is running
```

### Port Already in Use
- Check if services are already running: `make.bat status`
- Stop conflicting services: `make.bat down`

## Performance Tuning

### Spark Configuration
Edit `spark-defaults.conf` to adjust:
- Memory allocation (`spark.executor.memory`)
- CPU cores (`spark.executor.cores`)
- Parallelism settings

### Kafka Configuration
Adjust in `compose.yml`:
- Partition count for topics
- Retention policies
- Memory settings

## 📄 License

This project is licensed under the MIT License.

## Documentation Support

For issues related to:
- **Spark**: Check [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- **Kafka**: Check [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- **Podman**: Check [Podman Documentation](https://docs.podman.io/)

---

