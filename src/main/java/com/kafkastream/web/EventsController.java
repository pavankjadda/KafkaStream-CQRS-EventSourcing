package com.kafkastream.web;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Order;
import com.kafkastream.service.EventsSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;

import java.util.concurrent.ExecutionException;

@Controller
public class EventsController
{
    private final EventsSender eventsSender;

    @Autowired
    public EventsController(EventsSender eventsSender)
    {
        this.eventsSender = eventsSender;
    }

    @GetMapping("/create-customer")
    public ModelAndView createCustomer()
    {
        return new ModelAndView("create-customer");
    }

    @GetMapping("/create-order")
    public ModelAndView createOrder()
    {
        return new ModelAndView("create-order");
    }

    @PostMapping("/create-customer")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ModelAndView createCustomer(@ModelAttribute Customer customer) throws ExecutionException, InterruptedException
    {
        eventsSender.sendCustomerEvent(customer);
        return new ModelAndView("redirect:http://localhost:8095/customers");
    }

    @PostMapping("/create-order")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ModelAndView createOrder(@ModelAttribute Order order) throws ExecutionException, InterruptedException
    {
        eventsSender.sendOrderEvent(order);
        return new ModelAndView("redirect:http://localhost:8095/orders");
    }
}
