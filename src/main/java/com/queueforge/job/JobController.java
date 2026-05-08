package com.queueforge.job;

import com.queueforge.job.dto.*;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> create(@Valid @RequestBody CreateJobRequest request) {
        Job saved = jobService.createAndEnqueue(request);
        return ResponseEntity
                .created(URI.create("/jobs/" + saved.getId()))
                .body(JobResponse.from(saved));
    }

    @PostMapping("/bulk")
    public ResponseEntity<List<JobResponse>> bulkCreate(@Valid @RequestBody BulkCreateJobRequest request) {
        List<JobResponse> created = jobService.bulkCreate(request).stream()
                .map(JobResponse::from)
                .toList();
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }

    @GetMapping("/{id}")
    public JobResponse getById(@PathVariable Long id) {
        return JobResponse.from(jobService.getById(id));
    }

    @GetMapping
    public PageResponse<JobResponse> list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String type,
            @PageableDefault(size = 20) Pageable pageable
    ) {
        Page<Job> page = jobService.search(status, type, pageable);
        return PageResponse.from(page, JobResponse::from);
    }

    @GetMapping("/stats")
    public JobStatsResponse stats() {
        return jobService.stats();
    }

    @PatchMapping("/{id}/status")
    public JobResponse updateStatus(@PathVariable Long id,
                                    @Valid @RequestBody UpdateStatusRequest body) {
        return JobResponse.from(jobService.updateStatus(id, body.getStatus()));
    }

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(@PathVariable Long id) {
        return JobResponse.from(jobService.cancel(id));
    }

    @PostMapping("/{id}/retry")
    public JobResponse retry(@PathVariable Long id) {
        return JobResponse.from(jobService.retry(id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable Long id) {
        jobService.delete(id);
        return ResponseEntity.noContent().build();
    }
}
