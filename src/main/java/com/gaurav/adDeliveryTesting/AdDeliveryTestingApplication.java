package com.gaurav.adDeliveryTesting;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class AdDeliveryTestingApplication {

	public static void main(String[] args) {
		SpringApplication.run(AdDeliveryTestingApplication.class, args);
	}
}
