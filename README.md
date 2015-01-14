
### Example project to integrate Kafka, Avro, Twitter and Spark Streaming

This is WIP.

Current infrastructure:
- Tweets are serialized to Avro (without code generation) and sent to Kafka
- A Kafka consumer picks up serialized Tweets and prints them to stdout

### How to run

1. Start Kafka [(instructions)](http://kafka.apache.org/documentation.html#introduction)

2. Run the project with Gradle
```
./gradlew run
```

### TODO
- Add Vagrantfile to provision 3-node Kafka cluster
- Attach Spark Streaming
