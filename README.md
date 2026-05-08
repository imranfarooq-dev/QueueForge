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
