package com.revature.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@SpringBootApplication
@RestController
@RequestMapping("/api/messages")
public class ProducerApp {

    private static final String TOPIC = "messages";

    // TODO: Inject KafkaTemplate
    // private final KafkaTemplate<String, String> kafkaTemplate;

    // TODO: Add constructor injection
    // public ProducerApp(KafkaTemplate<String, String> kafkaTemplate) {
    // this.kafkaTemplate = kafkaTemplate;
    // }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApp.class, args);
    }

    @PostMapping
    public Map<String, String> sendMessage(@RequestBody Map<String, String> payload) {
        String message = payload.get("message");

        // TODO: Send message to Kafka
        // kafkaTemplate.send(TOPIC, message);

        System.out.println("Sending message: " + message);

        return Map.of(
                "status", "sent",
                "topic", TOPIC,
                "message", message);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "producer");
    }
}
