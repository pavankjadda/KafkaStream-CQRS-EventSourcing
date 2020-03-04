package com.kafkastream.service;

import com.kafkastream.dto.CustomerDTO;
import com.kafkastream.dto.CustomerOrderDTO;
import com.kafkastream.dto.GreetingDTO;
import com.kafkastream.dto.OrderDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
public class CustomRestTemplateService
{
    private final RestTemplate restTemplate;

    @Autowired
    public CustomRestTemplateService(RestTemplate restTemplate)
    {
        this.restTemplate = restTemplate;
    }

    public List<CustomerDTO> getAllCustomers()
    {
        ResponseEntity<List<CustomerDTO>> response=restTemplate.exchange("http://localhost:8095/store/customers",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<CustomerDTO>>(){});
        return response.getBody();
    }

    public List<OrderDTO> getAllOrders()
    {
        ResponseEntity<List<OrderDTO>> response=restTemplate.exchange("http://localhost:8095/store/orders",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<OrderDTO>>(){});
        return response.getBody();
    }

    public List<GreetingDTO> getAllGreetings()
    {
        ResponseEntity<List<GreetingDTO>> response=restTemplate.exchange("http://localhost:8095/store/greetings",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<GreetingDTO>>(){});
        return response.getBody();
    }

    public List<CustomerOrderDTO> getAllCustomersOrders()
    {
        ResponseEntity<List<CustomerOrderDTO>> response=restTemplate.exchange("http://localhost:8095/store/customer" +
                        "-order/all",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<CustomerOrderDTO>>(){});
        return response.getBody();
    }
}
