package com.queueforge.job.event;

import com.queueforge.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Demo consumer: keeps an in-memory ring of the last N events received
 * so {@code GET /admin/kafka/events/recent} can show what flowed through.
 * Real consumption + processing lives in the future Worker service.
 */
@Component
public class JobEventListener {

    private static final Logger log = LoggerFactory.getLogger(JobEventListener.class);
    private static final int RING_CAPACITY = 100;

    private final Deque<String> recent = new ConcurrentLinkedDeque<>();

    @KafkaListener(topics = KafkaConfig.JOB_EVENTS_TOPIC, groupId = "queueforge-app-demo")
    public void onEvent(String payload) {
        log.info("kafka event received: {}", payload);
        recent.addFirst(payload);
        while (recent.size() > RING_CAPACITY) {
            recent.removeLast();
        }
    }

    public List<String> recent() {
        return List.copyOf(recent);
    }
}
