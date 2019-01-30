package com.kafkastream.statestore;

import com.kafkastream.constants.KafkaConstants;
import com.kafkastream.dto.CustomerOrderDTO;
import com.kafkastream.model.CustomerOrder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.springframework.stereotype.Service;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;


@Service
public class StateStoreService
{

    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();

    public StateStoreService()
    {
    }


    public List<CustomerOrderDTO> getCustomerOrders(String customerId)
    {
        List<CustomerOrder> customerOrderList = client.target(String.format("http://%s:%d/%s", KafkaConstants.REST_PROXY_HOST, KafkaConstants.REST_PROXY_PORT, "customer-orders/all")).request(MediaType.APPLICATION_JSON_TYPE).get(new GenericType<List<CustomerOrder>>()
        {
        });

        return convertCustomerOrderListToCustomerOrderDTOList(customerOrderList);
    }

    private List<CustomerOrderDTO> convertCustomerOrderListToCustomerOrderDTOList(List<CustomerOrder> customerOrdersList)
    {
        List<CustomerOrderDTO> customerOrderDTOList = new ArrayList<>();
        for (CustomerOrder customerOrder : customerOrdersList)
        {
            customerOrderDTOList.add(new CustomerOrderDTO(customerOrder.getCustomerId().toString(), customerOrder.getFirstName().toString(), customerOrder.getLastName().toString(), customerOrder.getEmail().toString(), customerOrder.getPhone().toString(), customerOrder.getOrderId().toString(), customerOrder.getOrderItemName().toString(), customerOrder.getOrderPlace().toString(), customerOrder.getOrderPurchaseTime().toString()));
        }
        return customerOrderDTOList;
    }

}
