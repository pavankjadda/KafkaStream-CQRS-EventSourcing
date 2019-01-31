package com.kafkastream.service;

import com.kafkastream.dto.CustomerDto;
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
        ResponseEntity<List<CustomerDto>> response=restTemplate.exchange("http://localhost:8095/store/greetings",
                HttpMethod.GET,null, new ParameterizedTypeReference<List<CustomerDto>>(){});
        return response.getBody();
    }
}
