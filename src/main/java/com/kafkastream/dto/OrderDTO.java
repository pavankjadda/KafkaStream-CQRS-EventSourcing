package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderDTO implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    private String orderId;
    private String customerId;
    private String orderItemName;
    private String orderPlace;
    private String orderPurchaseTime;

    public OrderDTO()
    {
    }

    public OrderDTO(String orderId, String customerId, String orderItemName, String orderPlace, String orderPurchaseTime)
    {
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderItemName = orderItemName;
        this.orderPlace = orderPlace;
        this.orderPurchaseTime = orderPurchaseTime;
    }
}
