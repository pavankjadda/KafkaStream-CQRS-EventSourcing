package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
public class CustomerOrderDTO implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    public String customerId;
     public String firstName;
     public String lastName;
     public String email;
     public String phone;
     public String orderId;
     public String orderItemName;
     public String orderPlace;
     public String orderPurchaseTime;

     public CustomerOrderDTO()
     {

     }
    public CustomerOrderDTO(String customerId, String firstName, String lastName, String email, String phone, String orderId, String orderItemName, String orderPlace, String orderPurchaseTime)
    {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
        this.orderId = orderId;
        this.orderItemName = orderItemName;
        this.orderPlace = orderPlace;
        this.orderPurchaseTime = orderPurchaseTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof CustomerOrderDTO)) return false;
        CustomerOrderDTO that = (CustomerOrderDTO) o;
        return Objects.equals(customerId, that.customerId) && Objects.equals(firstName, that.firstName) && Objects.equals(lastName, that.lastName) && Objects.equals(email, that.email) && Objects.equals(phone, that.phone) && Objects.equals(orderId, that.orderId) && Objects.equals(orderItemName, that.orderItemName) && Objects.equals(orderPlace, that.orderPlace) && Objects.equals(orderPurchaseTime, that.orderPurchaseTime);
    }

    @Override
    public int hashCode()
    {

        return Objects.hash(customerId, firstName, lastName, email, phone, orderId, orderItemName, orderPlace, orderPurchaseTime);
    }
}
