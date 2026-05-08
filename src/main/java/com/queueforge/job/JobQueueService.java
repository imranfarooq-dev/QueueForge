package com.queueforge.job;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class JobQueueService {

    public static final String QUEUE_KEY = "queueforge:jobs";

    private final RedisTemplate<String, String> redisTemplate;

    public JobQueueService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void pushJob(String jobId) {
        redisTemplate.opsForList().leftPush(QUEUE_KEY, jobId);
    }

    public Long queueLength() {
        return redisTemplate.opsForList().size(QUEUE_KEY);
    }
}
