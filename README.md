
### Example project to integrate Kafka, Avro and Spark Streaming with Twitter as a stream source 

This is WIP.

Current infrastructure:
- Tweets are serialized to Avro (without code generation) and sent to Kafka
- A Kafka consumer picks up serialized Tweets and prints them to stdout

### How to run

1. Get Twitter credentials and fill them in `reference.conf`.

2. Start Kafka [(instructions)](http://kafka.apache.org/documentation.html#introduction) in single-node mode on localhost

3. Start Kafka producer
```
./gradlew produce 
```
This will start to read recent tweets, encode them to Avro and send to the Kafka cluster in binary format (`Array[Byte]`). 

4. Start Kafka consumer
```
 ./gradlew consume
```
This will run Spark streaming connected to the Kafka cluster. In 5-second intervals 
the program reads Avro tweets from Kafka, deserializes the tweet texts to strings 
and print 10 most frequent words
 

