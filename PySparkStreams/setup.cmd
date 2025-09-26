@echo off
REM setup.cmd - Initial setup for PySpark 3.5 LTS environment
REM Run this script first to create necessary directories and files

echo Setting up PySpark 3.5 LTS development environment...
echo.

REM Create necessary directories
echo Creating directories...
if not exist "src" mkdir src
if not exist "data" mkdir data
if not exist "output" mkdir output
if not exist "checkpoints" mkdir checkpoints
if not exist "logs" mkdir logs
if not exist "notebooks" mkdir notebooks

REM Create logs subdirectories
if not exist "logs\spark-events" mkdir logs\spark-events

REM Check if required files exist
echo Checking required files...
if not exist "compose.yml" (
    echo ERROR: compose.yml not found!
    echo Please ensure all files are copied to this directory.
    pause
    exit /b 1
)

if not exist "Dockerfile" (
    echo ERROR: Dockerfile not found!
    echo Please ensure all files are copied to this directory.
    pause
    exit /b 1
)

if not exist "requirements.txt" (
    echo ERROR: requirements.txt not found!
    echo Please ensure all files are copied to this directory.
    pause
    exit /b 1
)

REM Create a simple example Python file
echo Creating example Python file...
echo # PySpark 3.5 LTS Example > src\example.py
echo import findspark >> src\example.py
echo findspark.init() >> src\example.py
echo. >> src\example.py
echo from pyspark.sql import SparkSession >> src\example.py
echo. >> src\example.py
echo def main(): >> src\example.py
echo     spark = SparkSession.builder.appName("Example").getOrCreate() >> src\example.py
echo     print("Spark version:", spark.version) >> src\example.py
echo     spark.stop() >> src\example.py
echo. >> src\example.py
echo if __name__ == "__main__": >> src\example.py
echo     main() >> src\example.py

REM Create a simple notebook
echo Creating example notebook...
echo { > notebooks\example.ipynb
echo   "cells": [ >> notebooks\example.ipynb
echo     { >> notebooks\example.ipynb
echo       "cell_type": "code", >> notebooks\example.ipynb
echo       "source": [ >> notebooks\example.ipynb
echo         "import findspark\n", >> notebooks\example.ipynb
echo         "findspark.init()\n", >> notebooks\example.ipynb
echo         "from pyspark.sql import SparkSession\n", >> notebooks\example.ipynb
echo         "spark = SparkSession.builder.appName('Notebook').getOrCreate()\n", >> notebooks\example.ipynb
echo         "print('Spark version:', spark.version)" >> notebooks\example.ipynb
echo       ] >> notebooks\example.ipynb
echo     } >> notebooks\example.ipynb
echo   ], >> notebooks\example.ipynb
echo   "metadata": {}, >> notebooks\example.ipynb
echo   "nbformat": 4, >> notebooks\example.ipynb
echo   "nbformat_minor": 4 >> notebooks\example.ipynb
echo } >> notebooks\example.ipynb

echo.
echo Setup completed successfully!
echo.
echo Next steps:
echo 1. Run: make.bat build
echo 2. Run: make.bat up
echo 3. Test: make.bat health
echo.
echo For help: make.bat help
echo.
pause