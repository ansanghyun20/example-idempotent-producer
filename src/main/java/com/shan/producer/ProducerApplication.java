package com.shan.producer;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.clients.producer.*;

@SpringBootApplication
public class ProducerApplication {

	// 멱등성 프로듀서
	// idempotent producer
	public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);

		HashMap<String, Object> props = new HashMap<>();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<>("TEST.TOPIC.1", null, Integer.toString(i));
			Future<RecordMetadata> future = producer.send(record); // 전송 요청
			try {
				RecordMetadata metadata = future.get();
				System.out.println("topic:    " + metadata.topic());
				System.out.println("partition : " + metadata.partition());
				System.out.println("offset    : " + metadata.offset());
				System.out.println("timestamp : " + metadata.timestamp());
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}

	}
}
