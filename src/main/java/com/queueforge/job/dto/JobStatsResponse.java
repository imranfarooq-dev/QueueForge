package com.queueforge.job.dto;

import java.util.Map;

public class JobStatsResponse {

    private long total;
    private Map<String, Long> byStatus;
    private Map<String, Long> byType;
    private Long queueLength;

    public JobStatsResponse(long total, Map<String, Long> byStatus, Map<String, Long> byType, Long queueLength) {
        this.total = total;
        this.byStatus = byStatus;
        this.byType = byType;
        this.queueLength = queueLength;
    }

    public long getTotal() { return total; }
    public Map<String, Long> getByStatus() { return byStatus; }
    public Map<String, Long> getByType() { return byType; }
    public Long getQueueLength() { return queueLength; }
}
