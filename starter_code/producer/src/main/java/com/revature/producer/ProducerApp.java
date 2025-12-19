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

    //private static final String TOPIC = "messages";

    // TODO: Inject KafkaTemplate
    private final KafkaTemplate<String, String> kafkaTemplate;

    // TODO: Add constructor injection
    public ProducerApp(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(ProducerApp.class, args);
    }

    @PostMapping
    public Map<String, String> sendMessage(@RequestBody Map<String, String> payload) {
        // 1. Get message from payload
        String message = payload.get("message");

        // 2. Get topic from payload
        // If the user doesn't provide a topic, default to "messages"
        String targetTopic = payload.getOrDefault("topic", "messages");

        // 3. Send message to the dynamic topic instead of the constant
        kafkaTemplate.send(targetTopic, message).whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("SUCCESS: Sent to [" + targetTopic + "] | Offset: " + result.getRecordMetadata().offset());
            } else {
                System.err.println("FAILURE: Error sending to [" + targetTopic + "]: " + ex.getMessage());
            }
        });

        // 4. Return the actual topic used in the response
        return Map.of(
                "status", "sent",
                "topic", targetTopic,
                "message", message);
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP", "service", "producer");
    }
}