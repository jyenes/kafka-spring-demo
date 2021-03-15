package com.jyenes.springkafka;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaSendCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

@SpringBootApplication
public class SpringKafkaApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringKafkaApplication.class);

	@KafkaListener(topics = "jyenes-topic", containerFactory = "listenerContainerFactory", groupId = "jyenes-group")
	public void listen(String message) {
		log.info("Message received {}", message);
	}

	@Autowired
	private KafkaTemplate<String , String>  kafkaTemplate;

	@Override
	public void run(String... args) throws Exception {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("jyenes-topic", "message");
		future.addCallback(new KafkaSendCallback<String, String>(){

			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message Sent {}", result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(KafkaProducerException ex) {
				log.error("Error Sending message ", ex);
			}
		});
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaApplication.class, args);
	}
}
