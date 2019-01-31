package com.kafkastream.service;

import com.kafkastream.dto.CustomerDto;
import com.kafkastream.dto.CustomerOrderDTO;
import com.kafkastream.dto.GreetingDto;
import com.kafkastream.dto.OrderDto;
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

    public List<CustomerDto> getAllCustomers()
    {
        ResponseEntity<List<CustomerDto>> response=restTemplate.exchange("http://localhost:8095/store/customers",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<CustomerDto>>(){});
        return response.getBody();
    }

    public List<OrderDto> getAllOrders()
    {
        ResponseEntity<List<OrderDto>> response=restTemplate.exchange("http://localhost:8095/store/orders",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<OrderDto>>(){});
        return response.getBody();
    }

    public List<GreetingDto> getAllGreetings()
    {
        ResponseEntity<List<GreetingDto>> response=restTemplate.exchange("http://localhost:8095/store/greetings",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<GreetingDto>>(){});
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
