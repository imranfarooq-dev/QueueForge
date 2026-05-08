package com.queueforge.job.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

public class BulkCreateJobRequest {

    @NotEmpty(message = "jobs list cannot be empty")
    @Size(max = 1000, message = "at most 1000 jobs per bulk request")
    @Valid
    private List<CreateJobRequest> jobs;

    public List<CreateJobRequest> getJobs() { return jobs; }
    public void setJobs(List<CreateJobRequest> jobs) { this.jobs = jobs; }
}
