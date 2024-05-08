package com.caltech.service;

import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import com.caltech.dto.User;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaProducerService {

	@Value("${kafka.topic.user.key.create}")
	private String createUserEvent;
	
	@Value("${kafka.topic.user.key.update}")
	private String updateUserEvent;
	
	@Value("${kafka.topic.user.key.delete}")
	private String deleteUserEvent;
	
	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;
	
	public void sendCreateEventForUser(User user) {
		String key = "create-" + user.getUserId();
		sendEvent(createUserEvent, key, user);
	}
	
	public void sendUpdateEventForUser(User user) {
		String key = "update-" + user.getUserId();
		sendEvent(updateUserEvent, key, user);
	}
	
	public void sendDeleteEventForUser(long userId) {
		String key = "delete-" + userId;
		sendEvent(deleteUserEvent, key, null);
	}
	
	private void sendEvent(String topic, String key, User user) {
		CompletableFuture<SendResult<String, User>> future = kafkaTemplate.send(topic, user);
		future.whenComplete((result, exception) -> {
			if (exception == null) {
				log.info("Send message [{}] to  topic [{}] with offset [{}]", user.toString(), topic, result.getRecordMetadata().offset());
			} else {
				log.error("Unable to send message [{}] to topic [{}] due to: {}", user.toString(), topic, exception.getMessage());
			}
		});
	}
}
