package com.kafkastream.service;

import com.kafkastream.event.CustomerEvent;
import com.kafkastream.event.GreetingsEvent;
import com.kafkastream.event.OrderEvent;
import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.kafkastream.stream.GenericStreams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;


@Service
@Slf4j
@EnableAutoConfiguration
public class EventsSender
{
    private GenericStreams genericStreams;

    @Autowired
    public EventsSender(GenericStreams   genericStreams)
    {
        this.genericStreams=genericStreams;
    }

    public void sendGreetingsEvent(Greetings greetings)
    {
        GreetingsEvent greetingsEvent = new GreetingsEvent(greetings, greetings.getMessage());
        Message<GreetingsEvent> message = MessageBuilder.withPayload(greetingsEvent).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingGreetings().send(message);
    }


    public void sendCustomerEvent(Customer  customer)
    {
        CustomerEvent customerEvent = new CustomerEvent(customer,customer.getCustomerId());
        Message<CustomerEvent> message = MessageBuilder.withPayload(customerEvent).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingCustomers().send(message);
    }

    public void sendOrderEvent(Order    order)
    {
        OrderEvent orderEvent = new OrderEvent(order,order.getOrderId());
        Message<OrderEvent> message = MessageBuilder.withPayload(orderEvent).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON).build();
        genericStreams.outgoingOrders().send(message);
    }

}
