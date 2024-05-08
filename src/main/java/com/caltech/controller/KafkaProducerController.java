package com.caltech.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.http.ResponseEntity.HeadersBuilder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.caltech.dto.User;
import com.caltech.service.KafkaProducerService;

@RestController
@RequestMapping("/producer")
public class KafkaProducerController {

    @Autowired
    private KafkaProducerService publisher;
    
    @PostMapping("/createUser")
    public ResponseEntity<String> sendcreateUserEvent(@RequestBody User user) {
    	try {
    		publisher.sendCreateEventForUser(user);
    		return ResponseEntity.ok("User " + user.toString() + " has been successfully created");
    	} catch (Exception e) {
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body("Failed to create User " + user.toString());
    	}
    }
    
    @PutMapping("/updateUser/{userId}")
    public ResponseEntity<String> sendupdateUserEvent(@PathVariable long userId, @RequestBody User user) {
    	try {
    		publisher.sendUpdateEventForUser(user);
    		return ResponseEntity.ok("User " + user.toString() + " has been successfully updated");
    	} catch (Exception e) {
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body("Failed to update User " + user.toString());
    	}
    }
    
    @DeleteMapping("/deleteUser/{userId}")
    public ResponseEntity<String> sendDeleteUserEvent(@PathVariable long userId) {
    	try {
    		publisher.sendDeleteEventForUser(userId);
    		return ResponseEntity.ok("User with userId " + userId + " has been successfully deleted");
    	} catch (Exception e) {
    		return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
    				.body("Failed to delete User with userId" + userId);
    	}
    }
}
