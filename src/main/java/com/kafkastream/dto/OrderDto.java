package com.kafkastream.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class OrderDto implements Serializable
{
    private static final long serialVersionUID = 2729048783015827572L;

    public String orderId;
    public String customerId;
    public String orderItemName;
    public String orderPlace;
    public String orderPurchaseTime;

    public OrderDto()
    {
    }

    public OrderDto(String orderId, String customerId, String orderItemName, String orderPlace, String orderPurchaseTime)
    {
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderItemName = orderItemName;
        this.orderPlace = orderPlace;
        this.orderPurchaseTime = orderPurchaseTime;
    }
}
