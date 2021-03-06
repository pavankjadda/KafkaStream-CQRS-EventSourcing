package com.pj.cqrs.kafkastream.service;

import com.pj.cqrs.kafkastream.model.Customer;
import com.pj.cqrs.kafkastream.model.CustomerOrder;
import com.pj.cqrs.kafkastream.model.Greetings;
import com.pj.cqrs.kafkastream.model.Order;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static com.pj.cqrs.kafkastream.constants.KafkaConstants.REST_PROXY_HOST;
import static com.pj.cqrs.kafkastream.constants.KafkaConstants.REST_PROXY_PORT;

//Custom RestTemplate Service to fetch data from Jersey REST Proxy API
@Service
public class CustomRestTemplateService
{
	private final RestTemplate restTemplate;

	public CustomRestTemplateService(RestTemplate restTemplate)
	{
		this.restTemplate = restTemplate;
	}

	public List<Customer> getAllCustomers()
	{
		ResponseEntity<List<Customer>> response = restTemplate.exchange(REST_PROXY_HOST + ":" + REST_PROXY_PORT + "/store/customers", HttpMethod.GET, null, new ParameterizedTypeReference<>()
		{
		});
		return response.getBody();
	}

	public List<Order> getAllOrders()
	{
		ResponseEntity<List<Order>> response = restTemplate.exchange(REST_PROXY_HOST + ":" + REST_PROXY_PORT + "/store/orders", HttpMethod.GET, null, new ParameterizedTypeReference<>()
		{
		});
		return response.getBody();
	}

	public List<Greetings> getAllGreetings()
	{
		ResponseEntity<List<Greetings>> response = restTemplate.exchange(REST_PROXY_HOST + ":" + REST_PROXY_PORT + "/store/greetings", HttpMethod.GET, null, new ParameterizedTypeReference<>()
		{
		});
		return response.getBody();
	}

	public List<CustomerOrder> getAllCustomersOrders()
	{
		ResponseEntity<List<CustomerOrder>> response = restTemplate.exchange(REST_PROXY_HOST + ":" + REST_PROXY_PORT + "/store/customer-order/all", HttpMethod.GET, null, new ParameterizedTypeReference<>()
		{
		});
		return response.getBody();
	}
}
