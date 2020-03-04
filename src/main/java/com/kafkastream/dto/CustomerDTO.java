package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class CustomerDTO implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    private String customerId;
    private String firstName;
    private String lastName;
    private String email;
    private String phone;

    public CustomerDTO(String customerId, String firstName, String lastName, String email, String phone)
    {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
    }
}
