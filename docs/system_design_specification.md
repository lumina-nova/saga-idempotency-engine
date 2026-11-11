# Architecture & Design Document: Distributed Saga & Idempotency Engine

## 1. System Overview & Guiding Philosophy

The **Saga & Idempotency Engine** provides application-level consistency and deduplication across distributed services. 

Distributed transactions fail in two predictable ways:
* **The Partial Execution Problem:** A workflow partially completes (e.g., payment succeeds, stock reservation fails), leaving persistent state inconsistent across microservice datastores.
* **The At-Least-Once Delivery Dilemma:** Network latency and transient blips trigger upstream retries, causing downstream side effects (e.g., charging accounts) to execute multiple times.

This project delivers an embedded, infrastructure-agnostic Java library providing deterministic forward orchestration, reverse compensation, and request deduplication. Version 1 focuses exclusively on zero-dependency state correctness using PostgreSQL ACID guarantees, avoiding external distributed lock managers or brokers.

---

## 2. Locked Architectural Decisions (v1 Baseline)

| # | Decision Topic | Selected Architectural Choice | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **1** | **Source of Truth for Idempotency** | PostgreSQL unique constraint on `(scope, idem_key)` | External distributed locks (e.g., Redis) can expire mid-execution during JVM pauses or network stalls. A database unique constraint cannot be bypassed or expired by clock anomalies. |
| **2** | **Delivery & Execution Guarantee** | At-least-once delivery + Idempotent handlers = **Effectively-Once** execution | Physical networks cannot achieve true "exactly-once" delivery across distinct TCP nodes. The engine guarantees deterministic at-least-once dispatch paired with strict deduplication guards. |
| **3** | **Saga Coordination Style** | **Orchestration** (Single dedicated coordinator per saga instance) | Centralized state tracking makes multi-step workflows inspectable, deterministically recoverable after crashes, and straightforward to trace compared to distributed choreography. |
| **4** | **Execution Model (v1)** | Synchronous steps executing in-process on caller thread | Avoids message broker complexity in the baseline release while proving state persistence, transaction boundaries, and automated reverse compensation. |
| **5** | **In-Flight Duplicate Handling** | Reject immediately with **HTTP 409 Conflict** & `Retry-After` header | Prevents thread starvation and unbounded request queueing. Callers receive explicit backoff instructions rather than hanging connections. |
| **6** | **Outcome Caching Strategy** | Cache deterministic business errors (4xx) as final; purge infrastructure errors (5xx, timeouts) | Business errors (e.g., `422 Insufficient Balance`) are terminal and must be replayed on retry. Infrastructure faults (e.g., socket reset) are transient and must allow subsequent retries. |
| **7** | **Compensation Contract** | Compensations must be **idempotent** and retry with backoff up to `max_attempts`; on exhaustion, flag `COMPENSATION_FAILED` | Rollback operations run over unstable networks and may be executed multiple times. Silent failures are prohibited; exhausted rollbacks require manual operational escalation. |
| **8** | **Non-Compensable Steps** | Explicit **Pivot Steps** supported in DSL | Steps with permanent external side effects (e.g., sending an SMS or submitting physical batch orders) cannot be rolled back. After a pivot step succeeds, the saga commits to forward retries only. |

---

## 3. Failure-Mode & Recovery Matrix

This matrix maps physical infrastructure failure points to engine behavior and verified mitigations:

| Failure Event | System Detection Mechanism | Engine State Transition | Automated Mitigation |
| :--- | :--- | :--- | :--- |
| **Caller drops connection mid-flight** | Upstream socket disconnect; step execution completes in background | Stays `PROCESSING` $\to$ moves to `COMPLETED` | Output payload remains safely cached in PostgreSQL. When the client retries with the same `Idempotency-Key`, the engine immediately returns the cached response. |
| **Coordinator instance crashes mid-step** | Heartbeat lease expires (`locked_until < NOW()`) | Stays `RUNNING` until claimed by sweeper | Background sweeper detects expired lease via `SELECT ... FOR UPDATE SKIP LOCKED`, acquires row ownership, increments `version`, and triggers reverse compensation. |
| **Downstream service timeout** | Socket read timeout or circuit breaker trip | Step marked `FAILED` $\to$ Saga marked `COMPENSATING` | Engine halts forward progress and executes reverse compensations for all previously completed steps in strict reverse sequence. |
| **Zombie worker resumes after timeout** | Monotonic version check / optimistic locking | Worker's final state update fails (0 rows affected) | The delayed thread detects that its fencing lease was reassigned, rolls back its local transaction, and discards its output without corrupting shared state. |
| **Compensation action fails temporarily** | Downstream service throws 503 or network error | Step stays `COMPENSATING`; increments `comp_attempts` | Engine applies exponential backoff with jitter and retries the compensation action. |
| **Compensation exhausts max retries** | Retry budget depleted (`comp_attempts >= max_attempts`) | Saga marked `COMPENSATION_FAILED` | State is committed durably to the audit log; alerts are emitted for human operator intervention. |
| **Same idempotency key used with different payload** | SHA-256 payload hash mismatch against stored record | Rejection | Request is rejected with **HTTP 422 Unprocessable Content**, preventing unintended parameter reuse. |

---

## 4. Architectural Comparison: Why Not Existing Engines?

| Dimension | Our Engine (v1/v2) | Temporal / Cadence | Camunda / Zeebe | Axon Framework |
| :--- | :--- | :--- | :--- | :--- |
| **Operational Footprint** | **Zero external servers** (Embedded library backed by existing Postgres) | Heavy (Requires dedicated Temporal server, cluster datastore, and workers) | Heavy (Requires dedicated Zeebe broker cluster or Camunda engine setup) | Medium/Heavy (Requires Axon Server or dedicated event store infra) |
| **Architecture Fit** | Standard Spring Boot application layer | Event-sourced external orchestrator | External BPMN engine | Full Event Sourcing & CQRS commitment required |
| **Deduplication Integration** | Native HTTP & Kafka idempotency guard included out of the box | Requires custom application-level workflow ID deduplication logic | Requires manual business key deduplication | Handled via aggregate command dispatching |
| **Learning Curve** | Minimal (Standard Java DSL + `@Idempotent` annotations) | High (Strict workflow determinism constraints, external SDKs) | High (BPMN modeling notation and XML/engine semantics) | High (Requires adopting full DDD/CQRS paradigm across codebase) |

---

## 5. Scope Boundaries

### What v1 Delivers:
* Pure Java core module with zero framework imports (enforced by Maven build rules).
* PostgreSQL-backed idempotency store using atomic `INSERT ... ON CONFLICT DO NOTHING`.
* Synchronous in-process Saga orchestrator with forward tracking and reverse compensation.
* Crash recovery sweeper using `FOR UPDATE SKIP LOCKED`.
* Full integration test suite using Testcontainers and Toxiproxy.

### What v2 Delivers:
* Asynchronous transport with Apache Kafka.
* Transactional Outbox Pattern to guarantee atomic database updates and event publishing.
* Read-through Redis cache layer for high-throughput replay of completed idempotency keys.
* Parallel step execution (fork/join).
* Micrometer metrics and OpenTelemetry trace propagation.