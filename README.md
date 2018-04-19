# CQRS Event Sourcing with Kafka Streaming
## What's this?
This repository demonstrates [CQRS](https://www.confluent.io/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/) Event Sourcing (Materialized views) with Kafka Streaming ([not Kafka messaging](https://github.com/pavankjadda/SpringCloudStream-Kafka))

## How to Run?
1. Download and install Kafka either from [Confluent](https://docs.confluent.io/current/installation/installing_cp.html#zip-and-tar-archives) or follow instructions [from here](https://www.tutorialspoint.com/apache_kafka/apache_kafka_installation_steps.htm) first. I recommend [Confluent](https://docs.confluent.io/current/installation/installing_cp.html#zip-and-tar-archives) as it combines all the servers into one package with additional tools.
2. Clone this repository and open in IntelliJ or Eclipse as maven project and run `KafkaStreamApplication` class. This will bring up producer class.
3. Open http://localhost:8090/orders or http://localhost:8090/customers or http://localhost:8090/sendevents to send kafka evensts (objects). 
4. Modify [code](https://github.com/pavankjadda/KafkaStream-CQRS-EventSourcing/blob/master/src/main/java/com/kafkastream/web/EventsController.java), if you want to send events with dofferent information.

## Technologies Used
1. Kafka Streams [(Confluent)](https://docs.confluent.io/current/installation/installing_cp.html#zip-and-tar-archives)
2. Confluent Schema Registry
3. Avro Serialization/Deserialization
4. Spring Boot

Note: For Kafka messaging implementation please look at this [repository](https://github.com/pavankjadda/SpringCloudStream-Kafka)
