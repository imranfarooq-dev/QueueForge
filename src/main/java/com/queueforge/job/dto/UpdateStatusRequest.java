package com.queueforge.job.dto;

import com.queueforge.job.JobStatus;
import jakarta.validation.constraints.NotNull;

public class UpdateStatusRequest {

    @NotNull(message = "status is required")
    private JobStatus status;

    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
}
