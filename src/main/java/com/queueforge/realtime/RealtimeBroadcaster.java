package com.queueforge.realtime;

import com.queueforge.job.JobQueueService;
import com.queueforge.job.dto.JobStatsResponse;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Single fan-out point for STOMP messages. Pages on the Angular client
 * subscribe to these destinations to get live updates without polling.
 */
@Component
public class RealtimeBroadcaster {

    public static final String TOPIC_JOBS = "/topic/jobs";
    public static final String TOPIC_METRICS = "/topic/metrics";
    public static final String TOPIC_QUEUE = "/topic/queue";

    private final SimpMessagingTemplate messagingTemplate;
    private final RedisTemplate<String, String> redisTemplate;
    private final JobQueueService jobQueueService;

    public RealtimeBroadcaster(SimpMessagingTemplate messagingTemplate,
                               RedisTemplate<String, String> redisTemplate,
                               @Lazy JobQueueService jobQueueService) {
        this.messagingTemplate = messagingTemplate;
        this.redisTemplate = redisTemplate;
        this.jobQueueService = jobQueueService;
    }

    /** Forwards a JobEvent JSON payload (already serialized) to /topic/jobs. */
    public void publishJobEvent(String jsonPayload) {
        messagingTemplate.convertAndSend(TOPIC_JOBS, jsonPayload);
    }

    public void publishMetrics(JobStatsResponse stats) {
        messagingTemplate.convertAndSend(TOPIC_METRICS, stats);
    }

    public void publishQueueSnapshot() {
        List<String> items = redisTemplate.opsForList().range(JobQueueService.QUEUE_KEY, 0, 49);
        Long length = jobQueueService.queueLength();
        Map<String, Object> body = Map.of(
                "key", JobQueueService.QUEUE_KEY,
                "length", length == null ? 0L : length,
                "items", items == null ? List.of() : items
        );
        messagingTemplate.convertAndSend(TOPIC_QUEUE, body);
    }
}
