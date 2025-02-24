# Command Line and Web App

## Running the Kafka Server Locally
To start the Kafka server on your local machine, follow these steps:

Verify Installation:
Ensure that Kafka is installed on your system and that the configuration file exists at the specified location.

Open Command Prompt:
Open your terminal or command prompt.

Run the Server:
Execute the following command to start the Kafka server:

kafka-server-start.bat C:\kafka\config\server.properties

Confirm Operation:
Check the console output to confirm that the server has started successfully.

For more details about Kafka configuration and troubleshooting, refer to the Apache Kafka Documentation.

##Below are step-by-step instructions to create a Kafka topic called livetemperature using the Kafka CLI:

Ensure Kafka is Running:
Make sure your Kafka broker is up and running. Typically, this means you have started the Kafka server (using kafka-server-start.bat or the equivalent script).

Open Your Terminal/Command Prompt:
Open a terminal (or command prompt if you’re on Windows) and navigate to the Kafka installation directory.

Create the Topic:
Depending on your operating system, run the appropriate command:
kafka-topics.bat --create --topic livetemperature --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

Verify the Topic Creation:
After running the command, list all topics to ensure that livetemperature was created successfully.

kafka-topics.bat --list --bootstrap-server localhost:9092



