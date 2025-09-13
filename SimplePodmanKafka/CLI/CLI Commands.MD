# Basic Command Structure

The base command you'll use to interact with the Kafka scripts inside your container is podman exec -it kafka-kraft. 
All subsequent Kafka commands will be appended to this. The primary script for managing topics is kafka-topics.

All topic management commands will follow this structure:

```bash
podman exec -it kafka-kraft kafka-topics --bootstrap-server localhost:9092 [command] [options]
```

* podman exec -it kafka-kraft: This part of the command uses Podman to execute a command inside the running container named kafka-kraft. The -it flags allow for interactive terminal access.
* kafka-topics: This is the Kafka script used for managing topics.
* --bootstrap-server localhost:9092: This option specifies the Kafka broker to connect to. In this case, it's connecting to a broker running on localhost at port 9092.
* [command]: This is where you'll specify the action you want to perform, such as --create, --list, or --delete.
* [options]: These are additional parameters that modify the behavior of the command, such as --topic to specify the topic name, --partitions to set the number of partitions, or --replication-factor to set the replication factor.

## Listing Topics
This command shows you all the topics that currently exist on your Kafka broker.
```bash
podman exec -it kafka-kraft kafka-topics --bootstrap-server localhost:9092 --list
``` 

Example Output:
```
my-first-topic
my-second-topic
``` 

## Creating a Topic
This command creates a new topic with the specified name, number of partitions, and replication factor.
```bash
podman exec -it kafka-kraft kafka-topics --bootstrap-server localhost:9092 --create --topic my-new-topic --partitions 3 --replication-factor 1
``` 
Example Output:
```
Created topic my-new-topic.
``` 
## Deleting a Topic
This command deletes an existing topic by name.
```bash
podman exec -it kafka-kraft kafka-topics --bootstrap-server localhost:9092 --delete --topic my-new-topic
``` 
Example Output:
```
Deleted topic my-new-topic.
``` 
## Describing a Topic
This command provides detailed information about a specific topic, including its partitions and their status.
```bash
podman exec -it kafka-kraft kafka-topics --bootstrap-server localhost:9092 --describe --topic my-first-topic
```
Example Output:
```
Topic: my-first-topic	PartitionCount: 3	ReplicationFactor: 1	Configs: 
    Topic: my-first-topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
    Topic: my-first-topic	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
    Topic: my-first-topic	Partition: 2    Leader: 1	Replicas: 1	Isr: 1
``` 
## Produce and Consume Messages 
To produce and consume messages, you can use the kafka-console-producer and kafka-console-consumer scripts. Here are examples of how to use them:
### Producing Messages
This command allows you to send messages to a specified topic.
```bash
podman exec -it kafka-kraft kafka-console-producer --broker-list localhost:9092 --topic my-first-topic
```
You can then type messages directly into the terminal. Press Ctrl+D to exit.
### Consuming Messages
This command allows you to read messages from a specified topic.
```bash
podman exec -it kafka-kraft kafka-console-consumer --bootstrap-server localhost:9092 --topic my-first-topic --from-beginning
```
This will display all messages from the beginning of the topic. Press Ctrl+C to exit.   
## Additional Options
You can explore more options and commands by using the help flag:
```bash
podman exec -it kafka-kraft kafka-topics --help
```
This will display a list of all available commands and options for managing Kafka topics.   
## Note
Make sure to replace topic names and other parameters with those that fit your specific use case.

