package com.queueforge.job;

import com.queueforge.job.dto.BulkCreateJobRequest;
import com.queueforge.job.dto.CreateJobRequest;
import com.queueforge.job.dto.JobStatsResponse;
import com.queueforge.job.event.JobEventPublisher;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final JobQueueService jobQueueService;
    private final JobEventPublisher jobEventPublisher;

    public JobService(JobRepository jobRepository,
                      JobQueueService jobQueueService,
                      JobEventPublisher jobEventPublisher) {
        this.jobRepository = jobRepository;
        this.jobQueueService = jobQueueService;
        this.jobEventPublisher = jobEventPublisher;
    }

    @Transactional
    public Job createAndEnqueue(CreateJobRequest request) {
        Job job = new Job();
        job.setType(request.getType());
        job.setPayload(request.getPayload());

        Job saved = jobRepository.save(job);
        jobQueueService.pushJob(saved.getId().toString());
        jobEventPublisher.publish(saved, "CREATED");
        return saved;
    }

    @Transactional
    public List<Job> bulkCreate(BulkCreateJobRequest request) {
        return request.getJobs().stream()
                .map(this::createAndEnqueue)
                .toList();
    }

    @Cacheable(value = "jobs", key = "#id")
    public Job getById(Long id) {
        return jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
    }

    public Page<Job> search(JobStatus status, String type, Pageable pageable) {
        return jobRepository.search(status, type, pageable);
    }

    public JobStatsResponse stats() {
        long total = jobRepository.count();

        Map<String, Long> byStatus = new EnumMap<>(JobStatus.class).keySet().isEmpty()
                ? new HashMap<>() : new HashMap<>();
        for (JobStatus s : JobStatus.values()) {
            byStatus.put(s.name(), 0L);
        }
        jobRepository.countByStatus().forEach(c -> byStatus.put(c.getStatus().name(), c.getCount()));

        Map<String, Long> byType = new HashMap<>();
        jobRepository.countByType().forEach(c -> byType.put(c.getType(), c.getCount()));

        Long queueLength = jobQueueService.queueLength();
        return new JobStatsResponse(total, byStatus, byType, queueLength);
    }

    @Transactional
    @CacheEvict(value = "jobs", key = "#id")
    public Job updateStatus(Long id, JobStatus status) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
        job.setStatus(status);
        Job saved = jobRepository.save(job);
        jobEventPublisher.publish(saved, "STATUS_CHANGED");
        return saved;
    }

    @Transactional
    @CacheEvict(value = "jobs", key = "#id")
    public Job cancel(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
        if (job.getStatus() != JobStatus.QUEUED && job.getStatus() != JobStatus.PROCESSING) {
            throw new InvalidJobStateException(
                    "Cannot cancel job " + id + " in status " + job.getStatus());
        }
        job.setStatus(JobStatus.CANCELLED);
        Job saved = jobRepository.save(job);
        jobEventPublisher.publish(saved, "CANCELLED");
        return saved;
    }

    @Transactional
    @CacheEvict(value = "jobs", key = "#id")
    public Job retry(Long id) {
        Job job = jobRepository.findById(id)
                .orElseThrow(() -> new JobNotFoundException(id));
        if (job.getStatus() != JobStatus.FAILED) {
            throw new InvalidJobStateException(
                    "Only FAILED jobs can be retried; job " + id + " is " + job.getStatus());
        }
        job.setStatus(JobStatus.QUEUED);
        job.setRetryCount(job.getRetryCount() + 1);
        Job saved = jobRepository.save(job);
        jobQueueService.pushJob(saved.getId().toString());
        jobEventPublisher.publish(saved, "RETRY");
        return saved;
    }

    @Transactional
    @CacheEvict(value = "jobs", key = "#id")
    public void delete(Long id) {
        if (!jobRepository.existsById(id)) {
            throw new JobNotFoundException(id);
        }
        jobRepository.deleteById(id);
    }
}
