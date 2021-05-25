package com.pj.cqrs.kafkastream.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ApplicationConfig implements WebMvcConfigurer
{
	private static final String[] CLASSPATH_RESOURCE_LOCATIONS =
			{
					"classpath:/META-INF/resources/", "classpath:/resources/", "classpath:/static/", "classpath:/public/", "classpath:/static/vendor/", "classpath:/static/custom/"
			};

	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry)
	{
		registry.addResourceHandler("/**").addResourceLocations(CLASSPATH_RESOURCE_LOCATIONS);
	}

	@Bean
	public RestTemplate restTemplate()
	{
		return new RestTemplate();
	}
}
