package com.queueforge.realtime;

import com.queueforge.job.JobService;
import com.queueforge.job.dto.JobStatsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodic heartbeat: pushes a fresh metrics + queue snapshot every N ms even
 * when no API call happens (e.g., direct DB writes). The {@link JobEventListener}
 * also calls {@link #publishNow()} on every Kafka event for instant updates.
 */
@Component
public class MetricsBroadcaster {

    private static final Logger log = LoggerFactory.getLogger(MetricsBroadcaster.class);

    private final JobService jobService;
    private final RealtimeBroadcaster broadcaster;

    public MetricsBroadcaster(JobService jobService, RealtimeBroadcaster broadcaster) {
        this.jobService = jobService;
        this.broadcaster = broadcaster;
    }

    @Scheduled(fixedDelayString = "${queueforge.metrics.interval-ms:5000}")
    public void heartbeat() {
        publishNow();
    }

    public void publishNow() {
        try {
            JobStatsResponse stats = jobService.stats();
            broadcaster.publishMetrics(stats);
            broadcaster.publishQueueSnapshot();
        } catch (Exception ex) {
            log.warn("Metrics broadcast failed: {}", ex.getMessage());
        }
    }
}
