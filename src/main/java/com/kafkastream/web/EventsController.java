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
import java.util.concurrent.ExecutionException;

@RestController
public class EventsController
{
    private EventsSender eventsSender;

    Random  random=new Random(1);

    @Autowired
    public EventsController(EventsSender    eventsSender)
    {
        this.eventsSender=eventsSender;
    }

    @GetMapping("/greetings")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Greetings sendGreetings() throws ExecutionException, InterruptedException
    {
        Random  random=new Random(1);
        String message = "Message "+random.nextInt();
        Greetings greetings = new Greetings(message,getCurrentTime());
        eventsSender.sendGreetingsEvent(greetings);
        return greetings;
    }

    @GetMapping("/customers")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Customer sendCustomers() throws ExecutionException, InterruptedException
    {
        //Send Customer Event
        Customer customer=new Customer();
        customer.setCustomerId("CU"+random.nextInt());
        customer.setFirstName("John");
        customer.setLastName("Doe");
        customer.setEmail("john.doe@gmail.com");
        customer.setPhone("993-332-9832");
        eventsSender.sendCustomerEvent(customer);
        return customer;
    }

    @GetMapping("/orders")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Order sendOrders() throws ExecutionException, InterruptedException
    {
        //Send Order Event
        Order order=new Order();
        order.setOrderId("ORD"+random.nextInt());
        order.setCustomerId("CU1001");
        order.setOrderItemName("Reebok Shoes");
        order.setOrderPlace("NewYork,NY");
        order.setOrderPurchaseTime(getCurrentTime());
        eventsSender.sendOrderEvent(order);
        return order;
    }

    @GetMapping("/sendevents")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void sendAllEvents() throws ExecutionException, InterruptedException
    {
        //Send Greetings event
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
        order.setOrderId("ORD"+random.nextInt());
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
