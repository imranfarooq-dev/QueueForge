package com.queueforge.job.event;

import com.queueforge.job.JobStatus;

import java.time.Instant;

public record JobEvent(
        Long jobId,
        String type,
        JobStatus status,
        String action,
        Instant timestamp
) {}
