package com.revature.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api/messages")
public class ConsumerApp {

    private final List<String> receivedMessages = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApp.class, args);
    }

    // 1. listener for "messages"
    @KafkaListener(topics = "messages", groupId = "message-consumers")
    public void listen(String message) {
        System.out.println("Received [Message]: " + message);
        receivedMessages.add("MESSAGE: " + message);
    }

    // 2. Listener for "orders"
    @KafkaListener(topics = "orders", groupId = "message-consumers")
    public void listenOrders(String message) {
        System.out.println("Received [Order]: " + message);
        receivedMessages.add("ORDER: " + message);
    }

    // 3. Listener for "notifications"
    @KafkaListener(topics = "notifications", groupId = "message-consumers")
    public void listenNotifications(String message) {
        System.out.println("Received [Notification]: " + message);
        receivedMessages.add("NOTIFICATION: " + message);
    }

    // Listener for all topics
    @KafkaListener(topics = {"messages", "orders", "notifications"}, groupId = "message-consumers")
    public void listenAll(String message) {
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