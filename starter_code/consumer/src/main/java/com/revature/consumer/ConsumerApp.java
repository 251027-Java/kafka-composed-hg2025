package com.revature.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api/messages")
public class ConsumerApp {

    private final List<String> receivedMessages = new ArrayList<>();

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApp.class, args);
    }

    // TODO: Add @KafkaListener annotation
    // @KafkaListener(topics = "messages", groupId = "message-consumers")
    public void listen(String message) {
        System.out.println("Received message: " + message);
        receivedMessages.add(message);
    }

    @GetMapping
    public Map<String, Object> getMessages() {
        return Map.of(
                "count", receivedMessages.size(),
                "messages", receivedMessages);
    }

    @DeleteMapping
    public Map<String, String> clearMessages() {
        receivedMessages.clear();
        return Map.of("status", "cleared");
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "consumer");
    }
}
