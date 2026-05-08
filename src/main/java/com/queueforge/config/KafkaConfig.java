package com.queueforge.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    public static final String JOB_EVENTS_TOPIC = "queueforge.jobs.events";

    @Bean
    public NewTopic jobEventsTopic() {
        return TopicBuilder.name(JOB_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
