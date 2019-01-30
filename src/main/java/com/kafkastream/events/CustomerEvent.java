package com.kafkastream.events;

import com.kafkastream.model.Customer;

public class CustomerEvent
{
    private Customer customer;

    //This is optional, we can ignore if we want
    private String customerId;

    public CustomerEvent()
    {
    }

    public CustomerEvent(Customer customer, String customerId)
    {
        this.customer = customer;
        this.customerId = customerId;
    }

    public Customer getCustomer()
    {
        return customer;
    }

    public void setCustomer(Customer customer)
    {
        this.customer = customer;
    }

    public String getCustomerId()
    {
        return customerId;
    }

    public void setCustomerId(String customerId)
    {
        this.customerId = customerId;
    }
}
