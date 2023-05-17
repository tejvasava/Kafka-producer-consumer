package com.paytm.main;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paytm.main.dto.PaymentRequest;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class PaytmConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaytmConsumerApplication.class, args);
	}

	@KafkaListener(topics="paytm",groupId="Payment-consumer-group")
	public void paymentConsumer(@Payload String paymentRequest) throws JsonProcessingException
	{
		//business logic
		
		//log.info("paymentConsumer {}",new ObjectMapper().writeValueAsString(paymentRequest));

		JsonNode data = new ObjectMapper().readTree(paymentRequest);
		log.info("paymentConsumer {}",data);
	}
}
