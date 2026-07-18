package com.example.incident_reponse_agent.service;


import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class IncidentProducerService {

    private static final String TOPIC = "security-alerts";
    private final KafkaTemplate<String , String > kafkaTemplate;


    public IncidentProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishAlert(String payload)
    {
        kafkaTemplate.send(TOPIC,payload);
    }
}
