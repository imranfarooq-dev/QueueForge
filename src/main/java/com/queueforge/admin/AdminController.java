package com.queueforge.admin;

import com.queueforge.config.KafkaConfig;
import com.queueforge.job.JobQueueService;
import com.queueforge.job.event.JobEventListener;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final RedisTemplate<String, String> redisTemplate;
    private final JobQueueService jobQueueService;
    private final KafkaAdmin kafkaAdmin;
    private final JobEventListener jobEventListener;

    public AdminController(RedisTemplate<String, String> redisTemplate,
                           JobQueueService jobQueueService,
                           KafkaAdmin kafkaAdmin,
                           JobEventListener jobEventListener) {
        this.redisTemplate = redisTemplate;
        this.jobQueueService = jobQueueService;
        this.kafkaAdmin = kafkaAdmin;
        this.jobEventListener = jobEventListener;
    }

    // ---------- Redis ----------

    @GetMapping("/redis/info")
    public Map<String, Object> redisInfo() {
        Properties info = redisTemplate.execute(
                (org.springframework.data.redis.core.RedisCallback<Properties>) connection ->
                        connection.serverCommands().info());
        Map<String, Object> result = new LinkedHashMap<>();
        if (info != null) {
            for (String key : List.of("redis_version", "uptime_in_seconds",
                    "connected_clients", "used_memory_human", "total_commands_processed")) {
                if (info.containsKey(key)) {
                    result.put(key, info.getProperty(key));
                }
            }
        }
        return result;
    }

    @GetMapping("/redis/keys")
    public List<String> redisKeys(@RequestParam(defaultValue = "queueforge:*") String pattern,
                                  @RequestParam(defaultValue = "100") int limit) {
        List<String> keys = new ArrayList<>();
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(100).build();
        try (Cursor<byte[]> cursor = Objects.requireNonNull(
                redisTemplate.getConnectionFactory()).getConnection().keyCommands().scan(options)) {
            while (cursor.hasNext() && keys.size() < limit) {
                keys.add(new String(cursor.next()));
            }
        }
        return keys;
    }

    // ---------- Queue (Redis list) ----------

    @GetMapping("/queue/length")
    public Map<String, Object> queueLength() {
        return Map.of(
                "key", JobQueueService.QUEUE_KEY,
                "length", Optional.ofNullable(jobQueueService.queueLength()).orElse(0L)
        );
    }

    @GetMapping("/queue")
    public Map<String, Object> queuePeek(@RequestParam(defaultValue = "0") long start,
                                         @RequestParam(defaultValue = "49") long end) {
        List<String> items = redisTemplate.opsForList().range(JobQueueService.QUEUE_KEY, start, end);
        return Map.of(
                "key", JobQueueService.QUEUE_KEY,
                "range", Map.of("start", start, "end", end),
                "items", items == null ? List.of() : items
        );
    }

    // ---------- Kafka ----------

    @GetMapping("/kafka/topics")
    public Map<String, Object> kafkaTopics() {
        Map<String, Object> response = new LinkedHashMap<>();
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Set<String> topics = admin.listTopics(new ListTopicsOptions().listInternal(false))
                    .names()
                    .get(5, TimeUnit.SECONDS);
            response.put("topics", new TreeSet<>(topics));
            response.put("trackedTopic", KafkaConfig.JOB_EVENTS_TOPIC);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            response.put("error", "Kafka not reachable: " + e.getMessage());
        }
        return response;
    }

    @GetMapping("/kafka/events/recent")
    public Map<String, Object> recentEvents() {
        List<String> recent = jobEventListener.recent();
        return Map.of(
                "topic", KafkaConfig.JOB_EVENTS_TOPIC,
                "count", recent.size(),
                "events", recent
        );
    }
}
