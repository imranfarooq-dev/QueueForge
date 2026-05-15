package com.queueforge.job.event;

import com.queueforge.config.KafkaConfig;
import com.queueforge.realtime.MetricsBroadcaster;
import com.queueforge.realtime.RealtimeBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Receives every Kafka event published by {@link JobEventPublisher} and:
 *   1. keeps an in-memory ring of the last N events for {@code GET /admin/kafka/events/recent}
 *   2. forwards each event to the STOMP broker so connected Angular clients see it instantly
 *   3. triggers a fresh metrics + queue snapshot push (so dashboards update on every change)
 */
@Component
public class JobEventListener {

    private static final Logger log = LoggerFactory.getLogger(JobEventListener.class);
    private static final int RING_CAPACITY = 100;

    private final Deque<String> recent = new ConcurrentLinkedDeque<>();
    private final RealtimeBroadcaster realtime;
    private final MetricsBroadcaster metricsBroadcaster;

    public JobEventListener(RealtimeBroadcaster realtime, MetricsBroadcaster metricsBroadcaster) {
        this.realtime = realtime;
        this.metricsBroadcaster = metricsBroadcaster;
    }

    @KafkaListener(topics = KafkaConfig.JOB_EVENTS_TOPIC, groupId = "queueforge-app-demo")
    public void onEvent(String payload) {
        log.info("kafka event received: {}", payload);
        recent.addFirst(payload);
        while (recent.size() > RING_CAPACITY) {
            recent.removeLast();
        }
        realtime.publishJobEvent(payload);
        metricsBroadcaster.publishNow();
    }

    public List<String> recent() {
        return List.copyOf(recent);
    }
}
