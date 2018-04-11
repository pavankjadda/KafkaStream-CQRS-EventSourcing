package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

import java.util.Properties;


public class EventsListener
{
    /*@StreamListener
    public void handleGreetings(@Input(GreetingsStreams.INPUT) KStream<String,GreetingsEvent> greetingsEventKStream)
    {
        greetingsEventKStream.foreach((key, value) -> System.out.println("Greetings Message: "+value.getMessage()));
    }*/

    //This method listens to events in Kafka topics and process them
    @SuppressWarnings(value = "unchecked")
    public static void main(String[] args)
    {
        Properties properties=new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"streams-greetings");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder streamsBuilder=new StreamsBuilder();

        KStream<String,Customer> customerKStream=streamsBuilder.stream("greetings");
        customerKStream.foreach(((key, value) -> System.out.println("customerKStream Value: "+value)));

        //Get data from greetings topic
      /*  KTable<String,Customer> customerKTable=streamsBuilder.table("customer");

        KTable<String,Order>   orderKTable=streamsBuilder.table("order");

        KTable<String,Greetings> greetingsKTable=streamsBuilder.table("greetings");

        KTable<String,Object>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order.getCustomerId()==customer.getCustomerId());

        customerKTable.foreach(((key, value) -> System.out.println("Value: "+value)));

        orderKTable.foreach(((key, value) -> System.out.println("Greetings Value: "+value)));*/
    }


}
