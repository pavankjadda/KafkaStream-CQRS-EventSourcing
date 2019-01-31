package com.kafkastream.web;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.kafkastream.service.CustomRestTemplateService;
import com.kafkastream.service.EventsSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.servlet.ModelAndView;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

@Controller
public class EventsController
{
    private final EventsSender eventsSender;

    private final CustomRestTemplateService customRestTemplateService;

    @Autowired
    public EventsController(EventsSender eventsSender, CustomRestTemplateService customRestTemplateService)
    {
        this.eventsSender = eventsSender;
        this.customRestTemplateService = customRestTemplateService;
    }

    @GetMapping(value = {"/","/home"})
    public ModelAndView getHome()
    {
        return new ModelAndView("home");
    }

    @GetMapping("/create-customer")
    public ModelAndView getCreateCustomer()
    {
        return new ModelAndView("create-customer");
    }

    @GetMapping("/create-order")
    public ModelAndView getCreateOrder()
    {
        return new ModelAndView("create-order");
    }

    @GetMapping("/create-greeting")
    public ModelAndView getCreateGreeting()
    {
        return new ModelAndView("create-greeting");
    }

    @GetMapping("/customers")
    public String getAllCustomers(Model model)
    {
        model.addAttribute("customers",customRestTemplateService.getAllCustomers());
        return "customers";
    }

    @GetMapping("/orders")
    public String getAllOrders(Model model)
    {
        model.addAttribute("orders",customRestTemplateService.getAllOrders());
        return "orders";
    }

    @GetMapping("/customer-orders/all")
    public String getAllCustomersOrders(Model model)
    {
        model.addAttribute("customer-orders",customRestTemplateService.getAllCustomersOrders());
        return "customer-orders";
    }

    @GetMapping("/greetings")
    public String getAllGreetings(Model model)
    {
        model.addAttribute("greetings",customRestTemplateService.getAllGreetings());
        return "greetings";
    }

    @PostMapping("/create-customer")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ModelAndView createCustomer(@ModelAttribute Customer customer) throws ExecutionException, InterruptedException
    {
        eventsSender.sendCustomerEvent(customer);

        ModelAndView modelAndView=new ModelAndView("new-customer");
        modelAndView.addObject(customer);
        return modelAndView;
    }

    @PostMapping("/create-order")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ModelAndView createOrder(@ModelAttribute Order order) throws ExecutionException, InterruptedException
    {
        order.setOrderPurchaseTime(LocalDateTime.now().toString());
        eventsSender.sendOrderEvent(order);

        ModelAndView modelAndView=new ModelAndView("new-order");
        modelAndView.addObject(order);
        return modelAndView;
    }

    @PostMapping("/create-greeting")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ModelAndView createGreeting(@ModelAttribute Greetings greetings) throws ExecutionException, InterruptedException
    {
        greetings.setTimestamp(LocalDateTime.now().toString());
        eventsSender.sendGreetingsEvent(greetings);

        ModelAndView modelAndView=new ModelAndView("new-greeting");
        modelAndView.addObject(greetings);
        return modelAndView;
    }
}
