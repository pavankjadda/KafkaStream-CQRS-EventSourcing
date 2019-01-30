# Event Sourcing and Materialized views with Kafka Streams

## Introduction
Kafka helps you to build fast, high through put, fault tolerance, scalable microservices and applications. Kafka Streams stores data in Kafka Clusters (Kafka State Stores) and gets data wicket fast. 

This repository demonstrates [CQRS](https://www.confluent.io/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/) Event Sourcing (Materialized views) with Kafka Streaming ([Version: 2.1.0](https://archive.apache.org/dist/kafka/2.1.0/RELEASE_NOTES.html))
```
1. In typical production environment, we have multiple microservices and we want to perform multiphase commit to each microservice databases. 
2. Let's say if you user wants place an order in eShopping application, we have different microservices do the following operations (mentioned high level tasks)
        - Check the inventory for the requested product (Inventory MicroService takes care of this) 
        - Check the if any Payment method available and process payment(Payments MicroService takes care of this) 
        - Get Shipping Address and Billing Address (Customer Management MicroService takes care of this)

3. If anyone of the above microservice fails, we want to roll back the transaction and roll back updates made to microservices
4. In this repository all of the above operations (except roll back, because it makes application big and complicated to execute) done through EventSourcing or Event Streaming. In simple words we split each transaction in to small operations and then process through multi phase commit. 
5. For the sake of simplicity, in this repository you can send customers, orders and greetings events through url and  processed in receiver then stored in Kafka State Stores, which then accessed through REST API implemented through Jetty Server (Not MicroServices REST API)
```


## How to Run?
1. Download and install Kafka either from [Confluent](https://docs.confluent.io/current/installation/installing_cp.html#zip-and-tar-archives) or follow instructions [from here](https://www.tutorialspoint.com/apache_kafka/apache_kafka_installation_steps.htm) first. I recommend [Confluent](https://docs.confluent.io/current/installation/installing_cp.html#zip-and-tar-archives) as it combines all the servers into one package with additional tools.
   start kafka with the following command
    ```
    <path-to-confluent>/bin/confluent start
    ```
2. Clone this repository and open in IntelliJ or Eclipse as maven project and run `KafkaStreamApplication` class. This will bring up producer class.
3. Go to http://localhost:9021 => Topics=> create topics `customer`, `order` and `order-to-ktable` (if they do not exist)
4. Go to EventsListener class and execute main method to start REST Proxy (Jetty) then access Kafka data through REST API
5. Open http://localhost:8090/orders or http://localhost:8090/customers or http://localhost:8090/sendevents to send kafka events (objects). 
6. Go to http://localhost:8095/customer-orders/all to see all customer orders fetched from Kafka State Store. See [StateStore Rest](https://github.com/pavankjadda/KafkaStream-CQRS-EventSourcing/blob/master/src/main/java/com/kafkastream/web/kafkarest/StateStoreRestService.java) Api for possible methods, you can customize it further
7. Modify [code](https://github.com/pavankjadda/KafkaStream-CQRS-EventSourcing/blob/master/src/main/java/com/kafkastream/web/EventsController.java), if you want to send events with different information.

## Technologies Used
1. Kafka Streams [(Confluent)](https://docs.confluent.io/current/platform.html)
2. Materialized views and Kafka State Stores
3. REST Api using Jetty Server
4. Confluent Schema Registry
5. Avro Serializer/Deserializer
6. Spring Boot
7. Java 8

Note: For Kafka messaging implementation please look at this [repository](https://github.com/pavankjadda/SpringCloudStream-Kafka)
