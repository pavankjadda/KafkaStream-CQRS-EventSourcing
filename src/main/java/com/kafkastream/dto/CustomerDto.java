package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class CustomerDto implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    public String customerId;

    public String firstName;

    public String lastName;

    public String email;

    public String phone;

    public CustomerDto()
    {
    }

    public CustomerDto(String customerId, String firstName, String lastName, String email, String phone)
    {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
    }
}
