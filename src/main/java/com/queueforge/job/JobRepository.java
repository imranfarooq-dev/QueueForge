package com.queueforge.job;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobRepository extends JpaRepository<Job, Long> {

    @Query("""
           SELECT j FROM Job j
           WHERE (:status IS NULL OR j.status = :status)
             AND (:type IS NULL OR j.type = :type)
           """)
    Page<Job> search(@Param("status") JobStatus status,
                     @Param("type") String type,
                     Pageable pageable);

    @Query("SELECT j.status AS status, COUNT(j) AS count FROM Job j GROUP BY j.status")
    List<StatusCount> countByStatus();

    @Query("SELECT j.type AS type, COUNT(j) AS count FROM Job j GROUP BY j.type")
    List<TypeCount> countByType();

    interface StatusCount {
        JobStatus getStatus();
        long getCount();
    }

    interface TypeCount {
        String getType();
        long getCount();
    }
}
