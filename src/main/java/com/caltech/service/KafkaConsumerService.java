package com.caltech.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.caltech.dto.User;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaConsumerService {
	@KafkaListener(topics = "${kafka.topic.user.key.create}",groupId = "${spring.kafka.consumer.group-id}")
	public void consumeCreateUserEvent(User user) {
		log.info("Received create user event: {}", user.toString());
	}
	
    @KafkaListener(topics = "${kafka.topic.user.key.update}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeUpdateUserEvent(User user) {
        log.info("Received update user event: {}", user.toString());
    }
    
    @KafkaListener(topics = "${kafka.topic.user.key.delete}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeDeleteUserEvent(User user) {
        log.info("Received delete user event: {}", user.toString());
    }
}
