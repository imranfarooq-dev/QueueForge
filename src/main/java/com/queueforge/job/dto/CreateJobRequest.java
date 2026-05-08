package com.queueforge.job.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public class CreateJobRequest {

    @NotBlank(message = "type is required")
    @Size(max = 64, message = "type must be at most 64 chars")
    private String type;

    @Size(max = 65535, message = "payload too large")
    private String payload;

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
}
