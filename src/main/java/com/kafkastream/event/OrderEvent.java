package com.kafkastream.event;

import com.kafkastream.model.Order;

public class OrderEvent
{
    private Order   order;

    //We can ignore this, not necessary
    private String  orderId;

    public OrderEvent()
    {
    }

    public OrderEvent(Order order, String orderId)
    {
        this.order = order;
        this.orderId = orderId;
    }

    public Order getOrder()
    {
        return order;
    }

    public void setOrder(Order order)
    {
        this.order = order;
    }

    public String getOrderId()
    {
        return orderId;
    }

    public void setOrderId(String orderId)
    {
        this.orderId = orderId;
    }
}
