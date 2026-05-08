package com.queueforge.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class QueueMetricsTask {

    private static final Logger log = LoggerFactory.getLogger(QueueMetricsTask.class);

    private final JobQueueService jobQueueService;

    public QueueMetricsTask(JobQueueService jobQueueService) {
        this.jobQueueService = jobQueueService;
    }

    @Scheduled(fixedDelayString = "${queueforge.metrics.interval-ms:30000}")
    public void logQueueDepth() {
        try {
            Long depth = jobQueueService.queueLength();
            log.info("queueforge metrics — redis queue depth = {}", depth == null ? 0 : depth);
        } catch (Exception ex) {
            log.warn("queueforge metrics — failed to read queue depth: {}", ex.getMessage());
        }
    }
}
