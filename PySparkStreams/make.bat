@echo off
REM make.bat - PySpark 3.5 LTS Development Automation for Windows 11
REM Usage: make.bat [command]
REM Compatible with PyCharm IDE and Podman Desktop

setlocal enabledelayedexpansion

if "%1"=="" goto help

REM Check if podman is available
where podman >NUL 2>&1
if errorlevel 1 (
    echo Error: Podman not found in PATH. Please install Podman Desktop.
    exit /b 1
)

goto %1 2>NUL || (
    echo Unknown command: %1
    goto help
)

:help
echo PySpark 3.5 LTS Development Commands
echo =====================================
echo.
echo Build Commands:
echo   build         - Build all containers
echo   build-app     - Build only PySpark application container
echo.
echo Service Commands:
echo   up            - Start all services
echo   down          - Stop all services
echo   restart       - Restart all services
echo   logs          - Show logs for all services
echo   logs-app      - Show PySpark application logs
echo   logs-kafka    - Show Kafka logs
echo.
echo Development Commands:
echo   shell         - Enter PySpark container shell
echo   jupyter       - Start Jupyter notebook
echo   spark-ui      - Open Spark UI in browser
echo   kafka-ui      - Open Kafka UI in browser
echo   ncat          - Start ncat listener for socket streaming
echo   test-socket   - Test socket connection
echo.
echo PyCharm Integration:
echo   pycharm       - Setup PyCharm remote interpreter
echo   debug         - Start services in debug mode
echo.
echo Utility Commands:
echo   status        - Show container status
echo   clean         - Clean up containers and volumes
echo   clean-all     - Clean everything including images
echo   health        - Check service health
echo   topics        - List Kafka topics
echo   create-topic  - Create a test Kafka topic
echo.
goto end

:build
echo Building all containers...
podman compose build
goto end

:build-app
echo Building PySpark application container...
podman compose build pyspark-app
goto end

:up
echo Starting all services...
podman compose up -d
echo Waiting for services to be ready...
timeout /t 10 /nobreak >NUL
call :status
echo.
echo Services started successfully!
echo Access URLs:
echo   Spark UI:     http://localhost:4040
echo   Kafka UI:     http://localhost:8080
echo   Jupyter:      http://localhost:8888
goto end

:down
echo Stopping all services...
podman compose down
goto end

:restart
echo Restarting all services...
podman compose restart
goto end

:logs
echo Showing logs for all services...
podman compose logs -f
goto end

:logs-app
echo Showing PySpark application logs...
podman compose logs -f pyspark-app
goto end

:logs-kafka
echo Showing Kafka logs...
podman compose logs -f kafka
goto end

:shell
echo Entering PySpark container shell...
podman compose exec pyspark-app /bin/bash
goto end

:jupyter
echo Starting Jupyter notebook...
podman compose exec -d pyspark-app sh -c "START_JUPYTER=true /app/startup.sh"
echo Jupyter available at: http://localhost:8888
goto end

:spark-ui
echo Opening Spark UI...
start http://localhost:4040
goto end

:kafka-ui
echo Opening Kafka UI...
start http://localhost:8080
goto end

:ncat
echo Starting ncat listener on port 9999...
podman compose exec pyspark-app nc -lk 9999
goto end

:test-socket
echo Testing socket connection...
echo Hello from socket! | podman compose exec -T pyspark-app nc localhost 9999
goto end

:pycharm
echo PyCharm Remote Interpreter Setup:
echo.
echo 1. Open PyCharm Settings
echo 2. Go to Project - Python Interpreter
echo 3. Click gear icon - Add...
echo 4. Select "Docker" or "Podman"
echo 5. Configure:
echo    - Server: podman
echo    - Image: pyspark-streaming
echo    - Python interpreter path: /usr/bin/python3
echo.
echo Container must be running first. Use 'make up' command.
goto end

:debug
echo Starting services in debug mode...
set SPARK_DRIVER_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
podman compose up -d
echo Debug port 5005 available for remote debugging
goto end

:status
echo Container Status:
podman compose ps
goto end

:health
echo Checking service health...
echo.
echo Kafka Health:
podman compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list >NUL 2>&1
if errorlevel 1 (
    echo Kafka: ERROR
) else (
    echo Kafka: OK
)
echo.
echo PySpark Health:
podman compose exec pyspark-app pyspark --version >NUL 2>&1
if errorlevel 1 (
    echo PySpark: ERROR
) else (
    echo PySpark: OK
)
goto end

:topics
echo Kafka Topics:
podman compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
goto end

:create-topic
set /p TOPIC_NAME="Enter topic name (default: test-topic): "
if "!TOPIC_NAME!"=="" set TOPIC_NAME=test-topic
echo Creating Kafka topic: !TOPIC_NAME!
podman compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic !TOPIC_NAME! --partitions 3 --replication-factor 1
goto end

:clean
echo Cleaning up containers and volumes...
podman compose down -v
podman system prune -f
goto end

:clean-all
echo WARNING: This will remove all containers, volumes, and images!
set /p CONFIRM="Are you sure? (y/N): "
if /i "!CONFIRM!"=="y" (
    podman compose down -v --rmi all
    podman system prune -af
    echo Cleanup completed!
) else (
    echo Cleanup cancelled.
)
goto end

:end
endlocal