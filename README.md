# QueueForge — Distributed Job Processing System

A Spring Boot job-processing backend that simulates real-world infrastructure used in modern SaaS platforms.

The current step (Step 2 expanded) implements the **Producer side** end-to-end:

- REST API to create / list / filter / update / cancel / retry / bulk-create / delete jobs
- Job persistence in **PostgreSQL** via Spring Data JPA + Hibernate
- Job IDs pushed to a **Redis list** (the worker queue)
- Job lifecycle events published to a **Kafka topic** (`queueforge.jobs.events`)
- Visualization endpoints to peek at Redis state, queue contents, Kafka topics, and recent events

A **Worker Service** (Step 3) will consume from Redis, process, and flip job status — coming next.

---

## Architecture

```
HTTP client
    │  POST /jobs, GET /jobs, /jobs/stats, /jobs/{id}/{cancel,retry,status}, ...
    ▼
GlobalExceptionHandler  ◄── @RestControllerAdvice (JSON ApiError)
    │
JobController       ──►  JobService (@Transactional)
                              │
                              ├──►  JobRepository (Spring Data JPA, @Query, projections)  ──►  PostgreSQL
                              │
                              ├──►  JobQueueService  ──►  Redis list "queueforge:jobs"
                              │
                              └──►  JobEventPublisher  ──►  Kafka topic "queueforge.jobs.events"
                                                              │
                                                              ▼
                                                       JobEventListener (in-memory ring)

QueueMetricsTask  ──►  @Scheduled — logs Redis queue depth every 30s
@Cacheable("jobs") on getById  ──►  Redis-backed cache (auto-evicted on writes)
```

---

## Concepts demonstrated

| Concept                                    | Where                                          |
|-------------------------------------------|------------------------------------------------|
| Layered architecture (controller → service → repository → entity) | `com.queueforge.job.*`                          |
| Bean Validation (`@Valid`, `@NotBlank`, ...) | `dto/CreateJobRequest`, `BulkCreateJobRequest`, `UpdateStatusRequest` |
| Global exception handling                 | `web/GlobalExceptionHandler` (`@RestControllerAdvice`) |
| Custom exceptions with HTTP mapping       | `JobNotFoundException` (404), `InvalidJobStateException` (409) |
| Pagination & sorting                      | `JobController#list` with `@PageableDefault`   |
| JPQL with optional params                 | `JobRepository#search`                         |
| Projection interfaces                     | `JobRepository.StatusCount`, `TypeCount`       |
| `@Enumerated(EnumType.STRING)` enum mapping | `Job#status`, `JobStatus`                       |
| Indexed columns                           | `Job` `idx_jobs_status`, `idx_jobs_type`        |
| Cache abstraction (Redis-backed)          | `@EnableCaching` + `@Cacheable("jobs", key="#id")` in `JobService` |
| Scheduled tasks                           | `@EnableScheduling` + `QueueMetricsTask`        |
| Kafka producer                            | `JobEventPublisher` (`KafkaTemplate`)           |
| Kafka consumer                            | `JobEventListener` (`@KafkaListener`)           |
| Topic auto-provisioning                   | `KafkaConfig#jobEventsTopic` (`NewTopic`)       |
| Externalised config (12-factor)           | `application.properties` `${ENV:default}` everywhere |

---

## Project layout

```
queueforge-parent/
├── src/main/java/com/queueforge/
│   ├── QueueforgeParentApplication.java       (@SpringBootApplication, @EnableCaching, @EnableScheduling)
│   ├── config/
│   │   ├── RedisConfig.java                   (RedisTemplate beans)
│   │   └── KafkaConfig.java                   (NewTopic auto-provisioning)
│   ├── job/
│   │   ├── Job.java                           (entity)
│   │   ├── JobStatus.java                     (enum)
│   │   ├── JobRepository.java                 (Spring Data JPA + @Query + projections)
│   │   ├── JobService.java                    (business logic, transactional, cached)
│   │   ├── JobQueueService.java               (Redis list producer)
│   │   ├── JobController.java                 (REST endpoints)
│   │   ├── JobNotFoundException.java          (→ 404)
│   │   ├── InvalidJobStateException.java      (→ 409)
│   │   ├── QueueMetricsTask.java              (@Scheduled)
│   │   ├── dto/{CreateJobRequest, BulkCreateJobRequest, UpdateStatusRequest, JobResponse, JobStatsResponse, PageResponse}.java
│   │   └── event/
│   │       ├── JobEvent.java                  (record)
│   │       ├── JobEventPublisher.java         (KafkaTemplate sender)
│   │       └── JobEventListener.java          (@KafkaListener + ring buffer)
│   ├── admin/
│   │   └── AdminController.java               (Redis / queue / Kafka visualization)
│   └── web/
│       ├── ApiError.java
│       └── GlobalExceptionHandler.java
├── src/main/resources/application.properties
├── Dockerfile
├── docker-compose.yml                         (postgres + redis + kafka KRaft + app)
├── k8s/                                       (namespace, configmap, secret, postgres+PVC, redis, kafka, app)
└── postman/QueueForge.postman_collection.json
```

---

## Prerequisites

- Java 21
- Docker + Docker Compose
- `kubectl` + a local cluster (minikube/kind/k3d) — only for the k8s path

---

## Run — Option A: app on host, datastores in Docker (fastest)

```bash
docker compose up -d postgres redis kafka
./mvnw spring-boot:run
```

Defaults from `application.properties`: DB on `localhost:5432`, Redis on `localhost:6379`, Kafka on `localhost:9094` (the EXTERNAL listener exposed by docker-compose).

## Run — Option B: everything in Docker Compose

```bash
docker compose --profile app up --build
```

Containers come up: `queueforge-postgres`, `queueforge-redis`, `queueforge-kafka`, `queueforge-app` (on `:8080`). Inside the compose network the app talks to `kafka:9092` (PLAINTEXT listener).

## Run — Option C: Kubernetes

```bash
eval $(minikube docker-env)            # or 'kind load docker-image' / 'k3d image import'
docker build -t queueforge-app:latest .
kubectl apply -f k8s/
kubectl -n queueforge rollout status deploy/queueforge-app
kubectl -n queueforge port-forward svc/queueforge-app 8080:80
```

---

## API reference

Base URL: `http://localhost:8080`. All bodies are JSON. Errors return a uniform `ApiError` shape (`status`, `error`, `message`, `path`, `details`, `timestamp`).

### Jobs

| Method | Path                       | Purpose                                     |
|--------|----------------------------|---------------------------------------------|
| POST   | `/jobs`                    | Create one job (`201 Created`)              |
| POST   | `/jobs/bulk`               | Create up to 1000 jobs in one request       |
| GET    | `/jobs`                    | List with `?status=&type=&page=&size=&sort=` |
| GET    | `/jobs/{id}`               | Fetch by id (cached)                        |
| GET    | `/jobs/stats`              | Aggregate counts + queue depth              |
| PATCH  | `/jobs/{id}/status`        | Update status (`{"status":"PROCESSING"}`)   |
| POST   | `/jobs/{id}/cancel`        | Cancel `QUEUED` or `PROCESSING` job (409 otherwise) |
| POST   | `/jobs/{id}/retry`         | Re-enqueue a `FAILED` job (409 otherwise)   |
| DELETE | `/jobs/{id}`               | Delete a job (`204 No Content`)             |

### Admin / Visualization

| Method | Path                         | What it shows                                        |
|--------|------------------------------|------------------------------------------------------|
| GET    | `/admin/redis/info`          | Selected fields from Redis `INFO`                    |
| GET    | `/admin/redis/keys`          | Keys via `SCAN ?pattern=&limit=`                     |
| GET    | `/admin/queue/length`        | `LLEN queueforge:jobs`                               |
| GET    | `/admin/queue`               | `LRANGE queueforge:jobs ?start= ?end=` (peek)        |
| GET    | `/admin/kafka/topics`        | All topics (excluding internal) via Kafka AdminClient |
| GET    | `/admin/kafka/events/recent` | Last 100 events the in-app listener has received     |

### Actuator

`/actuator/health`, `/actuator/info`, `/actuator/metrics` are exposed. Liveness/readiness probes used by k8s come from `/actuator/health/{liveness,readiness}`.

---

## Quick smoke test

```bash
# 1. create a job
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"EMAIL","payload":"{\"to\":\"a@x.com\"}"}'

# 2. list QUEUED EMAIL jobs
curl 'http://localhost:8080/jobs?status=QUEUED&type=EMAIL&page=0&size=10'

# 3. inspect Redis queue
curl http://localhost:8080/admin/queue/length
curl 'http://localhost:8080/admin/queue?start=0&end=20'

# 4. inspect Kafka
curl http://localhost:8080/admin/kafka/topics
curl http://localhost:8080/admin/kafka/events/recent

# 5. stats (counts by status & type, queue depth)
curl http://localhost:8080/jobs/stats
```

A Postman collection (`postman/QueueForge.postman_collection.json`) covers all of these — Create Job auto-saves `{{jobId}}` so the rest just work.

---

## Usage flow — end to end

A guided walkthrough of the complete lifecycle of a job. Each step shows the command, what happens internally, and how to verify it. Until the Worker Service exists (Step 3), you'll **simulate the worker** by flipping status manually with `PATCH`.

### Step 0 — start everything

```bash
cd queueforge-parent

# bring up infra (postgres + redis + kafka). app stays off so you can run it on your host.
docker compose up -d postgres redis kafka

# wait until all three are healthy
docker compose ps

# start the Spring Boot app on the host
./mvnw spring-boot:run
```

Sanity check the app is alive:

```bash
curl http://localhost:8080/actuator/health
# -> {"status":"UP","components":{"db":{...},"redis":{...},"kafka":{...}}}
```

If `db`, `redis`, or `kafka` is `DOWN`, fix infra before continuing — the rest of the flow won't work.

---

### Step 1 — create a job (Producer in action)

```bash
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"EMAIL","payload":"{\"to\":\"abdul@libertysupply.com\",\"subject\":\"Welcome\"}"}'
```

Expected: `201 Created`. Response body has the new `id` (let's say `1`) and `"status": "QUEUED"`.

**What just happened — three side-effects in one request:**

```
POST /jobs
   │
   ▼
JobController.create()
   │
   ▼
JobService.createAndEnqueue()    @Transactional
   │
   ├──►  jobRepository.save(job)             ──► INSERT INTO jobs ...        (PostgreSQL)
   │
   ├──►  jobQueueService.pushJob(id)         ──► LPUSH queueforge:jobs 1     (Redis)
   │
   └──►  jobEventPublisher.publish(job, "CREATED")  ──► topic queueforge.jobs.events  (Kafka)
```

> **About transactionality:** the DB save runs inside the transaction. The Redis push and Kafka publish run inside the same method but are **not** rolled back on failure — that's by design (eventual consistency). Real systems often add the **transactional outbox** pattern to fix this; out of scope here.

---

### Step 2 — verify the three side-effects

**A. PostgreSQL row:**
```bash
curl http://localhost:8080/jobs/1
# or directly in the database:
docker exec -it queueforge-postgres \
  psql -U queueforge -d queueforge -c 'SELECT id, type, status FROM jobs;'
```

**B. Redis queue contains the job id:**
```bash
curl http://localhost:8080/admin/queue/length
# {"key":"queueforge:jobs","length":1}

curl 'http://localhost:8080/admin/queue?start=0&end=20'
# {"key":"queueforge:jobs","items":["1"]}

# or directly:
docker exec -it queueforge-redis redis-cli LRANGE queueforge:jobs 0 -1
```

**C. Kafka event was published and the in-app listener received it:**
```bash
curl http://localhost:8080/admin/kafka/topics
# {"topics":["queueforge.jobs.events"], ...}

curl http://localhost:8080/admin/kafka/events/recent
# {"topic":"queueforge.jobs.events","count":1,"events":["{\"jobId\":1,\"action\":\"CREATED\",...}"]}
```

You've now seen the producer side write to **all three** datastores from a single API call.

---

### Step 3 — simulate the worker (until Step 3 of the project lands)

A real worker would `BRPOP` from `queueforge:jobs`, process the payload, then update the DB. We'll fake that with `PATCH`:

```bash
# move to PROCESSING — the worker has picked up the job
curl -X PATCH http://localhost:8080/jobs/1/status \
  -H 'Content-Type: application/json' -d '{"status":"PROCESSING"}'

# move to DONE — the worker finished successfully
curl -X PATCH http://localhost:8080/jobs/1/status \
  -H 'Content-Type: application/json' -d '{"status":"DONE"}'
```

Each `PATCH` evicts the cache (`@CacheEvict`) and emits a `STATUS_CHANGED` event to Kafka. Verify:

```bash
curl http://localhost:8080/admin/kafka/events/recent
# now shows 3 events: CREATED, STATUS_CHANGED (PROCESSING), STATUS_CHANGED (DONE)

curl http://localhost:8080/jobs/stats
# byStatus.DONE goes up; byStatus.QUEUED goes down
```

> Note: the Redis list still has the id `"1"` because we never popped it. A real worker would `BRPOP` to atomically remove it. Inspect with `curl http://localhost:8080/admin/queue/length` — once the Worker Service exists, this will drop to 0 as work is consumed.

---

### Step 4 — failure + retry path

Force a failure, then retry:

```bash
# create a fresh job
JOB_ID=$(curl -s -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"IMAGE_PROCESSING","payload":"{\"url\":\"img.png\"}"}' | jq -r .id)

# worker hits an error
curl -X PATCH http://localhost:8080/jobs/$JOB_ID/status \
  -H 'Content-Type: application/json' -d '{"status":"FAILED"}'

# retry — moves it back to QUEUED, increments retryCount, pushes to Redis again
curl -X POST http://localhost:8080/jobs/$JOB_ID/retry

# inspect
curl http://localhost:8080/jobs/$JOB_ID
# {"status":"QUEUED","retryCount":1,...}
```

Try to retry a non-FAILED job → `409 Conflict` with a uniform `ApiError` body. Try to cancel a `DONE` job → also `409`. That logic lives in `JobService` (`cancel`, `retry`).

---

### Step 5 — bulk create + filter + paginate

```bash
# create 3 jobs at once
curl -X POST http://localhost:8080/jobs/bulk \
  -H 'Content-Type: application/json' \
  -d '{
    "jobs": [
      {"type":"EMAIL","payload":"{\"to\":\"a@x.com\"}"},
      {"type":"EMAIL","payload":"{\"to\":\"b@x.com\"}"},
      {"type":"REPORT","payload":"{\"reportId\":42}"}
    ]
  }'

# filter: only QUEUED EMAIL jobs, newest first, page 0
curl 'http://localhost:8080/jobs?status=QUEUED&type=EMAIL&page=0&size=20&sort=id,desc'
# returns { content: [...], page: 0, size: 20, totalElements: N, totalPages: M, last: ... }

# stats roll up everything
curl http://localhost:8080/jobs/stats
# {"total":N,"byStatus":{"QUEUED":...,"DONE":...},"byType":{"EMAIL":...,"REPORT":...},"queueLength":...}
```

---

### Step 6 — observe the scheduled task

`QueueMetricsTask` logs queue depth every 30 seconds. Watch the app logs — you'll see lines like:

```
INFO  --- c.q.j.QueueMetricsTask : queueforge metrics — redis queue depth = 4
```

That's `@Scheduled` in action. Tune the interval via `queueforge.metrics.interval-ms` in `application.properties` or as an env var.

---

### Step 7 — see error handling in action

```bash
# 400: missing required 'type'
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' -d '{"payload":"hi"}'

# 404: unknown id
curl -i http://localhost:8080/jobs/999999

# 409: invalid state transition
curl -i -X POST http://localhost:8080/jobs/1/retry   # 1 is DONE, not FAILED
```

Every error returns the same `ApiError` shape:

```json
{
  "timestamp": "2026-05-09T10:30:00Z",
  "status": 400,
  "error": "Bad Request",
  "message": "Validation failed",
  "path": "/jobs",
  "details": ["type: type is required"]
}
```

---

### One-glance map: which endpoint exercises which concept

| Action | Endpoint | Demonstrates |
|---|---|---|
| Create | `POST /jobs` | Validation, transactional save, Redis push, Kafka publish |
| Bulk create | `POST /jobs/bulk` | Nested validation (`@Valid` on list elements) |
| Read | `GET /jobs/{id}` | `@Cacheable` (second call hits Redis cache, not DB) |
| List | `GET /jobs` | `Pageable` + JPQL with optional params |
| Stats | `GET /jobs/stats` | Projection interfaces + `GROUP BY` aggregates |
| Update status | `PATCH /jobs/{id}/status` | `@CacheEvict`, Kafka event |
| Cancel / Retry | `POST /jobs/{id}/{cancel,retry}` | State-machine guards (409 on illegal transition) |
| Delete | `DELETE /jobs/{id}` | 204 No Content + cache evict |
| Queue peek | `GET /admin/queue` | Redis `LRANGE` without popping |
| Redis info / keys | `GET /admin/redis/{info,keys}` | Redis `INFO`, `SCAN` (production-safe) |
| Kafka topics | `GET /admin/kafka/topics` | Kafka `AdminClient` |
| Kafka events | `GET /admin/kafka/events/recent` | `@KafkaListener` + in-memory ring |

---

## Validation & error examples

```bash
# missing 'type' -> 400 with details
curl -i -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' -d '{"payload":"hi"}'

# unknown id -> 404
curl -i http://localhost:8080/jobs/999999

# cancel a DONE job -> 409
curl -i -X POST http://localhost:8080/jobs/1/cancel
```

All return uniform `ApiError` JSON.

---

## Future steps

- **Step 3 — Worker Service:** consume from Redis (`BRPOP`) or Kafka, process with retries, update DB status.
- Replace ad-hoc Redis list with full **Kafka-driven** queue (current setup keeps both).
- WebSocket push for real-time status updates.
- Spring Security + JWT for authn/authz.
- Prometheus/Grafana observability + distributed tracing.
- Kafka DLQ + scheduled job ttls.
