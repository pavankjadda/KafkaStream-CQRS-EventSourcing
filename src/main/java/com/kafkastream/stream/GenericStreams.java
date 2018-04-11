package com.kafkastream.stream;

import com.kafkastream.event.CustomerEvent;
import com.kafkastream.event.GreetingsEvent;
import com.kafkastream.event.OrderEvent;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface GenericStreams
{
    String Customer_input = "customers-in";
    String Customer_output = "customers-out";

    String Order_input = "orders-in";
    String Order_output = "orders-out";

    String Greetings_input = "greetings-in";
    String Greetings_output = "greetings-out";



    @Input(Order_input)
    //KStream<String, OrderEvent> incomingOrders();
    KTable<String, OrderEvent> incomingOrders();

    @Output(Order_output)
    MessageChannel outgoingOrders();

    @Input(Customer_input)
    KTable<String, CustomerEvent> incomingCustomers();

    @Output(Customer_output)
    MessageChannel outgoingCustomers();

    @Input(Greetings_input)
    KTable<String, GreetingsEvent> incomingGreetings();

    @Output(Greetings_output)
    MessageChannel outgoingGreetings();

}
