package com.queueforge.job.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.queueforge.config.KafkaConfig;
import com.queueforge.job.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class JobEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(JobEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public JobEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publish(Job job, String action) {
        JobEvent event = new JobEvent(
                job.getId(),
                job.getType(),
                job.getStatus(),
                action,
                Instant.now()
        );
        try {
            String payload = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(KafkaConfig.JOB_EVENTS_TOPIC, job.getId().toString(), payload);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize job event for jobId={}", job.getId(), e);
        } catch (Exception e) {
            log.warn("Failed to publish job event for jobId={} (kafka unavailable?)", job.getId(), e);
        }
    }
}
