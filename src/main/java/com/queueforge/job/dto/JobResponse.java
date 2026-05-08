package com.queueforge.job.dto;

import com.queueforge.job.Job;
import com.queueforge.job.JobStatus;

import java.time.LocalDateTime;

public class JobResponse {

    private Long id;
    private String type;
    private JobStatus status;
    private String payload;
    private int retryCount;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public static JobResponse from(Job job) {
        JobResponse r = new JobResponse();
        r.id = job.getId();
        r.type = job.getType();
        r.status = job.getStatus();
        r.payload = job.getPayload();
        r.retryCount = job.getRetryCount();
        r.createdAt = job.getCreatedAt();
        r.updatedAt = job.getUpdatedAt();
        return r;
    }

    public Long getId() { return id; }
    public String getType() { return type; }
    public JobStatus getStatus() { return status; }
    public String getPayload() { return payload; }
    public int getRetryCount() { return retryCount; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
}
