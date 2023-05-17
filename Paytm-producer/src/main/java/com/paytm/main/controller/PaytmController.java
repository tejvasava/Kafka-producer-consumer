package com.paytm.main.controller;

import java.util.Date;
import java.util.Random;

import org.apache.kafka.common.Uuid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.paytm.main.dto.PaymentRequest;
import com.paytm.main.dto.PaytmRequest;

@RestController
public class PaytmController {
	
	@Value("${paytm.producer.topic.name}")
	private String topicName;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;
	
	@GetMapping("/publish/{message}")
	public void sendMessage(@PathVariable String message)
	{
		kafkaTemplate.send(topicName, message);
	}
	
	@PostMapping("/paytm/payment")
	public String doPayment(@RequestBody PaytmRequest<PaymentRequest> paytmRequest)
	{
		for(int i=0;i<=500;i++)
		{
		PaymentRequest paymentRequest=paytmRequest.getPayload();
		paymentRequest.setDestAccount("DEST_ACT"+i);
		paymentRequest.setSrcAccouont("SRC_ACT"+i);
		paymentRequest.setTxnAmount(new Random().nextInt(1000));
		paymentRequest.setTransactionId(Uuid.randomUuid().toString());
		paymentRequest.setTxnDate(new Date());
		kafkaTemplate.send(topicName, paymentRequest);
		}
		return "payment instantiate succssfully";
	}
}
