package com.kafkastream.web;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.kafkastream.service.EventsSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Calendar;
import java.util.Random;
import java.util.TimeZone;

@RestController
public class EventsController
{
    private EventsSender eventsSender;

    @Autowired
    public EventsController(EventsSender    eventsSender)
    {
        this.eventsSender=eventsSender;
    }

    @GetMapping("/greetings")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Greetings send()
    {
        Random  random=new Random();
        String message = "Message "+random.nextInt();
        Greetings greetings = new Greetings(message,getCurrentTime());
        eventsSender.sendGreetingsEvent(greetings);
        return greetings;
    }


    @GetMapping("/sendevents")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void sendAllEvents()
    {
        //Send Greetings event
        Random random=new Random();
        String message = "Message "+random.nextInt();
        Greetings greetings = new Greetings(message,getCurrentTime());
        eventsSender.sendGreetingsEvent(greetings);

        //Send Customer Event
        Customer customer=new Customer();
        customer.setCustomerId("CU1001");
        customer.setFirstName("John");
        customer.setLastName("Doe");
        customer.setEmail("john.doe@gmail.com");
        customer.setPhone("993-332-9832");
        eventsSender.sendCustomerEvent(customer);

        //Send Order Event
        Order order=new Order();
        order.setOrderId("ORD1001");
        order.setCustomerId("CU1001");
        order.setOrderItemName("Reebok Shoes");
        order.setOrderPlace("NewYork,NY");
        order.setOrderPurchaseTime(getCurrentTime());
        eventsSender.sendOrderEvent(order);
    }

    private String getCurrentTime()
    {
        Calendar calendar=Calendar.getInstance(TimeZone.getDefault());
        return calendar.getTime().toString();
    }
}
