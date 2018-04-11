package com.kafkastream.model;

import java.util.Objects;

public class Order
{
    public String orderId;

    public String customerId;

    public String orderItemName;

    public String orderPlace;

    public String orderPurchaseTime;

    public Order()
    {
    }

    public Order(String orderId, String customerId, String orderItemName, String orderPlace, String orderPurchaseTime)
    {
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderItemName = orderItemName;
        this.orderPlace = orderPlace;
        this.orderPurchaseTime=orderPurchaseTime;
    }

    public String getOrderId()
    {
        return orderId;
    }

    public void setOrderId(String orderId)
    {
        this.orderId = orderId;
    }

    public String getCustomerId()
    {
        return customerId;
    }

    public void setCustomerId(String customerId)
    {
        this.customerId = customerId;
    }

    public String getOrderItemName()
    {
        return orderItemName;
    }

    public void setOrderItemName(String orderItemName)
    {
        this.orderItemName = orderItemName;
    }

    public String getOrderPlace()
    {
        return orderPlace;
    }

    public void setOrderPlace(String orderPlace)
    {
        this.orderPlace = orderPlace;
    }

    public String getOrderPurchaseTime()
    {
        return orderPurchaseTime;
    }

    public void setOrderPurchaseTime(String orderPurchaseTime)
    {
        this.orderPurchaseTime = orderPurchaseTime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(orderId, order.orderId) && Objects.equals(customerId, order.customerId) && Objects.equals(orderItemName, order.orderItemName) && Objects.equals(orderPlace, order.orderPlace) && Objects.equals(orderPurchaseTime, order.orderPurchaseTime);
    }

    @Override
    public int hashCode()
    {

        return Objects.hash(orderId, customerId, orderItemName, orderPlace, orderPurchaseTime);
    }

    @Override
    public String toString()
    {
        return "Order{" + "orderId='" + orderId + '\'' + ", customerId='" + customerId + '\'' + ", orderItemName='" + orderItemName + '\'' + ", orderPlace='" + orderPlace + '\'' + ", orderPurchaseTime='" + orderPurchaseTime + '\'' + '}';
    }
}
