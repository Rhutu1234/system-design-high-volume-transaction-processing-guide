# System Design: High-Volume Transaction Processing

*A capstone system design walkthrough — designing a system that processes a very large number of transactions per second — covering sharding and partitioning strategies, idempotency and exactly-once-effect guarantees under at-least-once delivery, the event log as the system's source of truth, coordinating writes across shards with sagas, concurrency control on hot rows, backpressure and load shedding, and the specific throughput-vs-correctness trade-offs that make high-volume transaction processing a uniquely demanding system design problem.*

---

## Table of Contents

1. [Introduction](#introduction)
2. [Why High-Volume Transaction Processing Is a Different Kind of Hard](#1-why-high-volume-transaction-processing-is-a-different-kind-of-hard)
3. [The Core Domain Model](#2-the-core-domain-model)
4. [The Event Log: Append-Only History as the Source of Truth](#3-the-event-log-append-only-history-as-the-source-of-truth)
5. [Idempotency: The Single Most Important Property](#4-idempotency-the-single-most-important-property)
6. [Sharding and Partitioning for Throughput](#5-sharding-and-partitioning-for-throughput)
7. [The Transaction State Machine](#6-the-transaction-state-machine)
8. [Concurrency Control on Hot Rows](#7-concurrency-control-on-hot-rows)
9. [The Saga: Coordinating Transactions Across Shards](#8-the-saga-coordinating-transactions-across-shards)
10. [Reconciliation](#9-reconciliation)
11. [Backpressure, Load Shedding, and Overload Protection](#10-backpressure-load-shedding-and-overload-protection)
12. [Data Security and Compliance](#11-data-security-and-compliance)
13. [Consistency, Availability, and the CAP Trade-off Under Load](#12-consistency-availability-and-the-cap-trade-off-under-load)
14. [Scaling the System](#13-scaling-the-system)
15. [Observability for a High-Throughput Transaction System](#14-observability-for-a-high-throughput-transaction-system)
16. [Common Pitfalls](#15-common-pitfalls)
17. [Quick Reference Table](#quick-reference-table)
18. [Conclusion](#conclusion)

---

## Introduction

A high-volume transaction processing system takes the general system design vocabulary covered in this series' System Design guide — databases, caching, queues, load balancing — and applies it under a throughput constraint most systems never face: tens or hundreds of thousands of state-changing writes per second, each of which must land exactly once, in the right order relative to other writes on the same entity, with no lost or duplicated effect. This guide walks through designing such a system end to end, drawing directly on this series' DDD, Event-Driven Architecture, Database Migrations, and Caching guides, each of which turns out to be load-bearing infrastructure for sustaining throughput without sacrificing correctness, rather than optional architectural polish.

```plaintext
Client → API Gateway → [idempotency check, rate limit] → Transaction Router → Shard (local ACID write)
                                                                ↓
                                                     Event Log (Kafka) — source of truth
                                                                ↓
                                                     Outbox → downstream consumers (analytics, notifications)
```

---

## 1. Why High-Volume Transaction Processing Is a Different Kind of Hard

### Throughput and correctness pull in opposite directions

Most systems covered in this series can trade a little correctness for a lot of throughput where the cost is bounded and recoverable — a stale cache entry, a slightly delayed notification. A high-volume transaction system's failure modes are different in kind: at 50,000 writes per second, a locking strategy or a coordination pattern that works fine at 500 TPS can fall over completely, and the fix is rarely "add more hardware" — it's rethinking which guarantees are enforced synchronously and which are enforced after the fact. This is why sharding (Section 5) and concurrency control (Section 7) dominate this guide's concerns more than any single database tuning knob does.

### At this scale, "rare" failure modes happen constantly

```plaintext
A race condition that fires once in 100,000 requests is a curiosity at 10 TPS (once every 3 hours).
The same race condition at 50,000 TPS fires roughly TWICE EVERY SECOND.
```

Unlike lower-throughput systems where an edge case can be deprioritized as "unlikely," volume itself turns low-probability bugs into constant, load-bearing behavior — this is why idempotency (Section 4) and explicit concurrency control (Section 7) are treated in this guide as first-class, non-negotiable design elements rather than defensive extras.

### You are almost never processing every transaction with the same code path

A critical, freeing realization for the design that follows: a high-volume transaction system, in the overwhelming majority of real-world designs, does not treat every transaction identically — the large majority of traffic (transfers between two accounts on the same shard, say) takes a cheap, fully local, single-partition path, while the minority that spans shards or requires coordination takes a more expensive, explicitly-designed-for path (Section 8). Optimizing the common case aggressively, rather than routing everything through the same general-purpose coordination logic, is precisely the kind of "know which path is hot" guidance echoed in this series' Caching and Database Indexing guides, applied here to transaction routing.

---

## 2. The Core Domain Model

### Modeled with DDD, per this series' companion guide

```csharp
public record TransactionId(Guid Value);
public record AccountId(Guid Value);
public record Money(long MinorUnits, string Currency); // see Section 3's note on this

public enum TransactionStatus { Pending, Applied, Failed, Reversed }

public class Transaction // the AGGREGATE ROOT, per this series' DDD guide
{
    public TransactionId Id { get; }
    public AccountId FromAccount { get; }
    public AccountId ToAccount { get; }
    public Money Amount { get; }
    public TransactionStatus Status { get; private set; }
    private readonly List<TransactionEvent> _domainEvents = new();

    public void Apply()
    {
        if (Status != TransactionStatus.Pending)
            throw new InvalidOperationException($"Cannot apply a transaction in status {Status}");
        Status = TransactionStatus.Applied;
        _domainEvents.Add(new TransactionAppliedEvent(Id, FromAccount, ToAccount, Amount));
    }

    public void Fail(string reason)
    {
        if (Status != TransactionStatus.Pending)
            throw new InvalidOperationException($"Cannot fail a transaction in status {Status}");
        Status = TransactionStatus.Failed;
        _domainEvents.Add(new TransactionFailedEvent(Id, reason));
    }
}
```

This directly applies this series' DDD guide's aggregate pattern — `Transaction` is the aggregate root, enforcing its own state transitions (you cannot apply a transaction twice) rather than trusting every caller to check status before mutating it, and raising domain events at exactly the points those transitions genuinely occur.

### Why the aggregate boundary should stay small at this scale

```csharp
// ❌ An aggregate spanning both accounts forces every transaction to lock two rows, killing throughput
// ✅ Transaction is its own aggregate; each Account is its own aggregate, updated via the transaction's effects
```

At low throughput, modeling a transfer as touching two `Account` aggregates directly inside one unit of work is convenient. At high volume, this is precisely where contention concentrates — per this series' DDD guide's aggregate-sizing discussion, keeping `Transaction` as its own aggregate, with `Account` balances updated as a *consequence* of applying it (Section 7), keeps the lock footprint of any single write as small as possible.

---

## 3. The Event Log: Append-Only History as the Source of Truth

### Why a mutable "current state" table alone is insufficient

```sql
-- ❌ Only ever knowing the CURRENT balance has no replay path and is trivially corruptible by a single bad UPDATE
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
```

A high-volume transaction system needs more than "what is the current state" — it needs an immutable, ordered, replayable record of *every* transaction that was ever accepted, and the ability to rebuild derived state (balances, aggregates, projections) from that record at any time. A mutable state table, updated in place, destroys that history the moment it's overwritten, and provides no structural way to recover from a bug that silently corrupted derived state.

### The log as the append-only backbone

```sql
CREATE TABLE transaction_log (
    sequence_id BIGINT PRIMARY KEY,      -- strictly increasing per partition
    transaction_id UUID NOT NULL,
    account_id UUID NOT NULL,            -- partition key: keeps one account's history strictly ordered
    amount_minor_units BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

In practice this table's role is usually filled by a distributed log (Kafka/Pulsar) rather than a relational table directly — every accepted transaction is first durably appended to the log, partitioned by `account_id`, *before* being applied to any derived balance store. This gives a durable, replayable record (rebuild the balance store from scratch if it's ever suspected corrupted), natural per-account ordering (single partition per account = strict order), and a backbone for downstream consumers via the **outbox/CDC pattern**, directly echoing this series' Event-Driven Architecture guide's discussion of avoiding dual-write inconsistency between "update the database" and "publish the event."

### Derived state is always recomputable, never independently authoritative

```sql
SELECT SUM(amount_minor_units) AS current_balance
FROM transaction_log
WHERE account_id = '...';
```

An account's current balance is, in principle, always a fold over its transaction log — never a separately-stored, independently-updatable number that could drift out of sync with the events that supposedly produced it. For performance (folding potentially millions of historical events on every read is genuinely expensive), a cached/materialized balance is a reasonable optimization (connecting directly to this series' Caching guide), but it must always be treated as a derived, re-verifiable projection of the log's truth — never the authoritative source itself.

---

## 4. Idempotency: The Single Most Important Property

### Why this is even more critical here than in most systems covered in this series

As covered throughout this series' RabbitMQ, Kafka, and Event-Driven Architecture guides, every messaging technology provides at-least-once delivery, and every network call can time out ambiguously (did the write actually commit server-side before the client gave up waiting?). At low throughput, a resulting duplicate is rare and often tolerable. At high volume, retries under load are *routine*, not exceptional — a slow shard, a transient network blip, a client-side timeout tuned too aggressively for peak load will all generate genuine retries constantly, which is precisely why idempotency is this guide's single most emphasized property.

### Idempotency keys: the standard mechanism

```csharp
[HttpPost("/transactions")]
public async Task<IActionResult> CreateTransaction(
    [FromHeader(Name = "Idempotency-Key")] string idempotencyKey,
    CreateTransactionRequest request)
{
    var existing = await _idempotencyStore.GetResultAsync(idempotencyKey);
    if (existing is not null)
    {
        return Ok(existing); // the SAME response as the original request, no reprocessing attempted
    }

    var result = await _transactionService.ProcessAsync(request);
    await _idempotencyStore.SaveResultAsync(idempotencyKey, result);
    return Ok(result);
}
```

This is the concrete implementation of the idempotency pattern introduced generally in this series' Redis guide's rate-limiting section and REST guide's discussion — a client generates a unique idempotency key per *logical* transaction attempt (not regenerated on retry) and includes it on every request, including retries; the server checks whether that key has already been processed and, if so, returns the *original* result rather than reprocessing. The idempotency store itself (typically Redis or a similarly fast key-value store) needs to sustain the system's full write rate on its own, which makes it a first-class scaling concern, not a lightweight side table.

### Idempotency at every layer the transaction touches, not just the outermost API

```plaintext
Client → API (idempotency key checked here)
            → Shard write (a database-level unique constraint on transaction_id prevents a duplicate insert)
            → Event published (per this series' Event-Driven Architecture guide, consumers must ALSO be idempotent)
            → Downstream projection update (must tolerate redelivery without double-applying)
```

Idempotency needs to be enforced at every hop, not just the client-facing entry point — the shard-level write should have a database constraint preventing a duplicate `transaction_id` from ever being inserted twice, and any downstream event consumers (per this series' Event-Driven Architecture guide) must independently be idempotent against redelivery, since at high volume "we'll just be extra careful" is not an acceptable substitute for structural, enforced guarantees at every layer.

---

## 5. Sharding and Partitioning for Throughput

### Why a single database can't sustain high-volume writes alone

```plaintext
A single primary database, however well-tuned, has a ceiling on write throughput —
determined by disk I/O, lock contention, and replication lag to standbys.
High-volume transaction processing means designing PAST that ceiling from the start.
```

As covered in this series' System Design guide's scaling discussion, vertical scaling (a bigger box) buys headroom but not a fundamentally higher ceiling — sustaining tens of thousands of writes per second requires horizontal partitioning, splitting the write workload across many independent shards that can each accept writes in parallel.

### Choosing the partition key: the decision that determines everything downstream

```plaintext
Shard by account_id (consistent hashing):
  → a single account's transactions always land on the same shard
  → balance updates for that account are a single-shard, local ACID transaction (cheap, fast)
  → a transfer between two accounts on DIFFERENT shards needs explicit coordination (Section 8)
```

As covered in this series' System Design and Cosmos DB/MongoDB guides, choosing the shard key deliberately — here, the account whose balance is most frequently read and written — avoids the expensive cross-shard fan-out that a poorly chosen key (transaction ID, say) would force on nearly every operation. The trade-off this choice deliberately accepts: same-shard transactions are cheap and local; cross-shard transactions (Section 8) are more expensive and require their own explicit design, so the partition key should be chosen to make the *common* case land on one shard as often as possible.

### Consistent hashing to avoid a costly resharding event

```csharp
// Consistent hashing minimizes the fraction of keys that need to move when a shard is added or removed
uint hash = ConsistentHash(accountId);
int shardIndex = _hashRing.GetShardForHash(hash);
```

Per this series' System Design guide's consistent-hashing discussion, a naive `hash(account_id) % shard_count` scheme requires remapping nearly every key whenever the shard count changes — consistent hashing (or a similar virtual-node scheme) keeps that remapping to a small fraction of keys, which matters considerably more here than in most systems, since a high-volume system is exactly the kind of system that eventually needs to add shards without a painful full-dataset migration.

---

## 6. The Transaction State Machine

### An explicit, enumerable set of states and legal transitions

```plaintext
Pending → Applied → (Reversed)
   ↓
 Failed
```

As covered in Section 2's `Transaction` aggregate, a transaction's lifecycle is a small, explicit state machine — and the aggregate's own methods (`Apply()`, `Fail()`) are what enforce that only legal transitions are ever possible, throwing rather than silently succeeding if called out of order (attempting to apply a transaction that already failed, for instance).

### Why an explicit state machine matters more here than for most domain objects

Given this guide's emphasis on volume turning rare bugs into routine, load-bearing behavior (Section 1), having every legal and illegal state transition explicitly enumerated and enforced by the aggregate itself — rather than scattered conditional checks across application code — is precisely the kind of rigor this series' DDD guide argues pays for itself most clearly in domains with genuinely high write concurrency, and few domains fit that description more clearly than high-volume transaction processing.

### Terminal states and their permanence

```csharp
public void Reverse(string reason)
{
    if (Status != TransactionStatus.Applied)
        throw new InvalidOperationException($"Cannot reverse a transaction in status {Status}");
    // ... an Applied transaction is reversed by a NEW compensating transaction, per Section 3 — never by mutating this one
}
```

Certain states are genuinely terminal (a `Failed` transaction doesn't transition anywhere further) — encoding these as hard constraints in the aggregate is what prevents an entire category of "this should never happen but somehow did" production incidents specific to concurrent, high-volume state changes.

---

## 7. Concurrency Control on Hot Rows

### Why the "average" contention rate doesn't tell the whole story

```plaintext
Most accounts: a handful of transactions per minute — contention is a non-issue.
A small number of HOT accounts (a popular merchant, a payroll disbursement account):
  thousands of concurrent writers hitting the SAME row, simultaneously.
```

At high volume, aggregate throughput numbers hide a skewed reality — a small number of hot rows can dominate contention even when the system's overall write rate is well within capacity, which is why concurrency control on individual rows deserves its own explicit design, not just "the database handles locking."

### Optimistic concurrency control for the common case

```csharp
// Read the current version, compute the new balance, write conditionally on that version
var account = await _repository.GetAsync(accountId);
var newBalance = account.Balance - amount;

var updated = await _db.ExecuteAsync(
    "UPDATE accounts SET balance = @newBalance, version = version + 1 WHERE id = @id AND version = @version",
    new { newBalance, id = accountId, version = account.Version });

if (updated == 0) throw new ConcurrencyConflictException(); // retry with backoff
```

**Optimistic concurrency control (OCC)** — read a version, compute the change, write conditionally on that version still matching — works well for the large majority of accounts, per this series' Database guide's discussion of OCC versus pessimistic locking, because it avoids holding a lock during any I/O and only pays a retry cost on genuine conflict, which for most rows is rare.

### Pessimistic locking and dedicated handling for genuinely hot rows

```plaintext
For a small, IDENTIFIABLE set of hot accounts:
  - route all writes for that account through a single, ordered queue (per-key serialization)
  - OR maintain the balance as a set of sharded sub-counters, reconciled asynchronously (Section 9)
  - OR maintain an append-only delta log for that account and materialize the balance periodically
```

For the identifiable minority of hot rows where OCC would mean constant retry storms, this series' Redis and Kafka guides' patterns for hot-key handling apply directly: serialize writes to that specific key through a single ordered path (a per-key queue or partition), or avoid a single mutable balance row entirely in favor of sharded counters or an append-only delta log reconciled asynchronously — the right choice depends on whether the account needs a synchronously-consistent balance or can tolerate eventual materialization.

---

## 8. The Saga: Coordinating Transactions Across Shards

### Why two-phase commit is usually the wrong tool at this scale

```plaintext
2PC: coordinator asks every shard to "prepare," then "commit" —
  couples the shards' availability together and holds locks across a network round trip.
At high throughput, this serializes exactly the work sharding was meant to parallelize.
```

As covered in this series' Distributed Systems and Event-Driven Architecture guides, two-phase commit provides a strong atomicity guarantee but does so by coupling participating shards' availability and latency together for the duration of the transaction — at high volume, this is precisely the kind of coordination cost that erodes the throughput sharding (Section 5) was meant to provide.

### The saga pattern: local ACID transactions plus explicit compensation

```plaintext
TransferSaga:
  1. Shard A: debit account (local ACID transaction) — compensating action: credit back
  2. Publish event: "debit applied"
  3. Shard B: credit account (local ACID transaction) — no compensation needed, this IS the completion
  4. On failure after step 1: run the compensating credit-back on Shard A
```

The **saga pattern**, covered generally in this series' Event-Driven Architecture and Microservices guides, replaces one large distributed transaction with a sequence of local, fast, single-shard transactions plus an explicit compensating action for anything that needs to be undone if a later step fails — favoring availability and throughput over the stronger, more expensive guarantee 2PC provides, and accepting a brief window where a cross-shard transfer is "in flight" in exchange for it.

### Sagas require idempotent, well-ordered compensation

```csharp
public async Task CompensateAsync(TransactionId id)
{
    var txn = await _repository.GetByIdAsync(id);
    var reversal = txn.Reverse("saga compensation: downstream leg failed"); // a NEW transaction, per Section 3
}
```

Unlike a database rollback, a saga's compensating action is itself a fully real, logged transaction (Section 3's append-only principle applies directly) — a failed downstream leg doesn't erase the original debit from history, it records a new, compensating credit alongside it, and that compensation must itself be idempotent (Section 4), since the saga orchestrator retrying a failed compensation step is exactly the kind of at-least-once delivery this whole guide assumes as a baseline.

---

## 9. Reconciliation

### Why "the derived balance looks right" isn't sufficient — it must be proven against the log

```plaintext
Cached balance for account X: $48,392.17
SUM over that account's transaction log entries: $48,392.17
→ these must match, and any drift is a genuine defect to find and explain, not a rounding error to absorb
```

**Reconciliation** is the (often continuous or nightly, automated) process of recomputing derived state directly from the append-only log and comparing it against whatever cached/materialized version the system actually serves reads from — this is the concrete, continuously-enforced verification that the log genuinely remains the source of truth, not just an assumption resting on the write path having worked correctly every time.

### Automating reconciliation, and surfacing discrepancies immediately

```csharp
public async Task ReconcileAsync(Guid accountId)
{
    var derivedBalance = await _transactionLog.SumEntriesAsync(accountId);
    var cachedBalance = await _balanceStore.GetBalanceAsync(accountId);

    if (derivedBalance != cachedBalance)
    {
        await _alerting.RaiseAsync("Reconciliation discrepancy detected", accountId); // per this series'
                                                                                        // Prometheus/Grafana guide's
                                                                                        // alerting discipline
    }
}
```

A discrepancy found during reconciliation — a cached balance that no longer matches the log it was derived from — is a genuinely serious signal, treated with the same urgency as a data-integrity incident rather than a routine data-quality issue to quietly patch; at high volume, catching this early matters more, since the number of affected transactions grows every second the drift goes unnoticed.

### Reconciliation as a recurring, automated background process

```csharp
public class ReconciliationWorker : BackgroundService // per this series' Background Services guide
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // scheduled and/or continuously streamed, per this series' Background Services guide's
        // recurring-job patterns and Kafka Streams-style continuous processing
    }
}
```

This directly reuses the scheduled background job patterns covered in this series' Background Services guide — reconciliation is precisely the kind of recurring, automated job those patterns are built for, run continuously or at short intervals given the volume involved, with its own health monitoring (per this series' Health Checks guide's "last successful run" pattern) to ensure the reconciliation job itself hasn't silently fallen behind.

---

## 10. Backpressure, Load Shedding, and Overload Protection

### Why "always accept the write" is the wrong default at high volume

```plaintext
A traffic spike well beyond provisioned capacity: accepting every request anyway
  degrades EVERY in-flight request's latency, including ones that would otherwise succeed fine.
Rejecting the excess FAST, at the edge, protects the requests the system can actually serve.
```

As covered in this series' Rate Limiting and Resilience (Polly/circuit breaker) guides, a system with a hard throughput ceiling needs an explicit answer for what happens when demand exceeds that ceiling — and "queue everything indefinitely" or "accept and hope" both tend to produce cascading failure under genuine overload, where latency degrades for all requests rather than a controlled subset being rejected.

### Rate limiting and queueing at the edge

```csharp
// Per this series' Redis-backed rate limiting guide, applied at the API gateway before the hot write path
if (!await _rateLimiter.TryAcquireAsync(clientId))
{
    return StatusCode(429, "Rate limit exceeded, retry with backoff");
}
```

Rate limiting at the API gateway, before a request ever reaches a shard, is the first line of defense — per this series' Redis guide's token-bucket/sliding-window patterns, applied per-client so that one high-volume caller can't starve every other client's fair share of capacity.

### Load shedding and circuit breaking under genuine overload

```plaintext
Queue depth on a shard exceeds a threshold → shed the LOWEST-priority traffic first
  (e.g., analytics writes before customer-facing writes), per this series' Resilience guide's
  circuit breaker and bulkhead patterns, isolating a struggling shard from taking down healthy ones.
```

Per this series' Resilience guide, a circuit breaker around a struggling downstream dependency (a specific overloaded shard, a slow fraud-check service) prevents that one component's degradation from cascading into every request that happens to touch it, and prioritized load shedding — deciding in advance which traffic classes are expendable under genuine overload — turns an undifferentiated outage into a controlled, partial degradation instead.

---

## 11. Data Security and Compliance

### Least-privilege access to shard-level data

The practical, almost universally adopted strategy at this scale mirrors this series' Secret Management and Identity guides' least-privilege principle: application services get narrowly-scoped credentials to only the shards and operations they genuinely need, and administrative access to raw shard data is separately audited and, wherever possible, avoided entirely in favor of tooling that operates through the same APIs and idempotency guarantees as normal traffic.

### Encryption and secret management for credentials and keys

```csharp
// Shard connection credentials are exactly the kind of secret covered in this series'
// Secret Management guide — never in source control, ideally via Managed Identity + Key Vault
var shardCredential = await _secretClient.GetSecretAsync($"shard-{shardId}-connection");
```

Every principle covered in this series' Secret Management guide applies directly and without exception here: no hardcoded credentials, Managed Identity where the platform supports it, and rotation discipline for anything that could grant an attacker write access to the transaction log or a shard's data at scale.

### Audit logging as a compliance and forensic requirement, not just an operational nicety

```csharp
logger.LogInformation("Transaction {TransactionId} applied for {Amount} on shard {ShardId}", txn.Id, txn.Amount, shardId);
```

As covered in this series' Structured Logging and OWASP Top 10 guides, every state-changing action needs to be logged with enough context (who, what, when, which shard) to support both regulatory audit requirements and forensic investigation after an incident — at high volume this logging itself becomes a throughput concern (Section 13), which is exactly why it needs to be designed for asynchronously, off the hot write path, rather than bolted on later.

---

## 12. Consistency, Availability, and the CAP Trade-off Under Load

### Why the write path favors consistency, even under load, while the read path doesn't have to

As covered in this series' System Design guide's CAP theorem discussion, the temptation under load is to relax consistency everywhere to preserve availability — but for the actual write that changes a balance, this guide follows the same reasoning covered in this series' Database guide's transaction-isolation discussion: it is generally preferable for a write to fail cleanly under contention (the client retries, per Section 4's idempotency guarantee) than for the system to accept it under uncertain conditions and risk a state discrepancy that reconciliation (Section 9) later has to painstakingly untangle.

### Where eventual consistency is deliberately, explicitly scoped in

```plaintext
The SHARD write (balance changed) → strong consistency required within that shard, no compromise
A downstream ANALYTICS dashboard showing "transactions per second right now" → eventual consistency is fine
A read replica serving "transaction history" queries → a few seconds of staleness is an acceptable trade
```

Not every part of a high-volume system needs the same consistency bar — the shard-local write absolutely does, but downstream, read-only projections (dashboards, history views, search indexes) can and should tolerate the eventual consistency this series' Event-Driven Architecture and CQRS discussions describe generally, since serving those reads from a strongly consistent path would only add latency and contention without a corresponding correctness benefit.

---

## 13. Scaling the System

### Applying this series' System Design guide's building blocks, with throughput-specific emphasis

```plaintext
Read replicas (per this series' SQL Server/PostgreSQL guides): safe for READ-heavy queries
  (transaction history, dashboards) — never route a WRITE that must be immediately consistent to a replica
Caching (per this series' Redis guide): appropriate for relatively static data and materialized balances
  read far more often than they change — always treated as a derived cache of the log (Section 3), never authoritative
Queues (per this series' RabbitMQ/Kafka guides): the backbone of the async parts of the flow
  (downstream projections, notifications, analytics) — the hot write path itself stays as short as possible
Batching (per this series' Kafka producer guide): batching log appends amortizes I/O cost significantly
  at high throughput, at the cost of a small, bounded increase in write latency
```

Every technique from this series' System Design guide applies here, with the caveat that each one needs to be evaluated against this guide's throughput requirements (Section 1) before being applied — the general principle "identify the bottleneck, then apply the specific technique" holds, but high volume narrows which techniques are safe on the hot path versus which belong strictly downstream of it.

### Read/write separation via CQRS

```plaintext
Writes → append to the log, apply to the shard-local balance store (Section 3, Section 7)
Reads  → served from a separately-scaled, denormalized query store, updated asynchronously from the log
```

Per this series' CQRS discussion (within the Event-Driven Architecture guide), separating the write model from the read model lets each be scaled and optimized independently — the write path stays minimal and fast, while the read path can be denormalized, cached, and horizontally replicated far more aggressively than the write path safely can be.

---

## 14. Observability for a High-Throughput Transaction System

### Every guide in this series' observability trio, applied with volume-specific stakes

```plaintext
Structured logs (per this series' Structured Logging guide): every transaction state transition, logged
  with the transaction ID, shard ID, and correlation ID — sampled or asynchronously batched at high volume,
  since logging every single event synchronously on the hot path would itself become the bottleneck
Distributed tracing (per this series' Distributed Tracing guide): tracing a single transaction's journey
  across routing, shard write, and log append — essential for diagnosing where a specific slow or
  failed transaction actually got stuck, especially in a saga (Section 8) spanning multiple shards
Metrics (per this series' Prometheus/Grafana guide): write throughput per shard, p99 write latency,
  conflict/retry rate (Section 7), reconciliation discrepancy count — the aggregate health signals
  an on-call engineer watches continuously
```

Every technique from this series' observability guides applies directly, with one throughput-specific addition worth stating explicitly: at this volume, observability itself must be designed to not become the bottleneck — sampling traces, batching log shipment, and aggregating metrics client-side before export are not optional optimizations but a genuine prerequisite for observability that doesn't degrade the very system it's meant to monitor.

### Alerting on shard-specific and system-wide symptoms

```promql
# Per this series' Prometheus/Grafana guide's symptom-based alerting principle, applied per shard
rate(transaction_conflict_total{shard="$shard"}[5m]) / rate(transaction_attempted_total{shard="$shard"}[5m]) > 0.05
```

A sudden spike in the conflict/retry rate on a specific shard (Section 7's hot-row concern surfacing in production), or a shard's write latency diverging from its peers, is exactly the kind of symptom this series' Prometheus/Grafana guide argues alerts should be built around — per-shard granularity matters here specifically because an aggregate, system-wide average can easily hide one struggling shard until it's already causing visible customer impact.

---

## 15. Common Pitfalls

| Pitfall | Why it hurts | Better approach |
|---|---|---|
| No idempotency key on transaction submission/retry | A network timeout retry genuinely double-applies the transaction | Idempotency keys enforced at every layer: API, shard write, and event publish |
| A single mutable balance column instead of an append-only log | No audit/replay trail; a single bad `UPDATE` silently corrupts derived state with no recovery path | Append-only transaction log; balances as a derived, recomputable projection |
| Sharding by transaction ID instead of account ID | Every balance read/write becomes a cross-shard fan-out | Shard by the entity whose state is read/written most often (account/entity ID) |
| Using two-phase commit for every cross-entity write | Couples shard availability together; serializes exactly what sharding was meant to parallelize | Local ACID transactions per shard + saga pattern with explicit compensation |
| Treating all rows as equally low-contention | A small number of hot rows dominate contention and silently throttle the whole system | Identify hot keys explicitly; use per-key serialization or sharded counters for them |
| No backpressure or load shedding under overload | Accepting every request during a spike degrades latency for all in-flight requests, including recoverable ones | Rate limit at the edge; shed low-priority traffic first under genuine overload |
| No reconciliation process, trusting the write path's correctness alone | A silent, undetected drift between derived state and the log it was supposed to reflect | Automated, continuous reconciliation comparing derived state against the log |
| Synchronous, unsampled logging/tracing on the hot write path | Observability itself becomes the throughput bottleneck | Asynchronous, batched, sampled telemetry off the hot path |

---

## Quick Reference Table

| Concept | Purpose |
|---|---|
| `Transaction` aggregate + state machine | Enforces only legal transaction state transitions, per this series' DDD guide |
| Append-only transaction log | The provably correct, replayable source of truth for all state changes |
| Idempotency key | Prevents duplicate application of a transaction from retries at every layer of the flow |
| Sharding by entity ID (consistent hashing) | Parallelizes write throughput while keeping the common case single-shard and cheap |
| Optimistic concurrency control | Handles the common, low-contention case without holding locks during I/O |
| Hot-key handling (per-key serialization / sharded counters) | Prevents a small number of contended rows from throttling the whole system |
| Saga + compensation | Coordinates a transaction correctly across shard boundaries without 2PC's coupling cost |
| Reconciliation | Continuously proves derived state matches the append-only log |
| Backpressure / load shedding | Protects overall system health under genuine overload, rather than degrading everything uniformly |

---

## Conclusion

A high-volume transaction processing system takes every general system design technique covered throughout this series and applies it under a throughput bar strict enough that volume itself becomes a first-class design constraint — because at tens of thousands of writes per second, coordination costs, lock contention, and low-probability edge cases stop being theoretical and start happening continuously. The design that actually holds up under that bar rests on a small number of non-negotiable foundations: an append-only event log as the provable source of truth; idempotency enforced at every single layer a transaction touches; sharding by the entity most frequently read and written, keeping the common case cheap and local; and continuous, automated reconciliation that treats any drift between derived state and the log as a genuine incident rather than noise to quietly absorb.

Nearly every architectural pattern covered elsewhere in this series shows up here in service of that bar — DDD's aggregates enforcing small, low-contention boundaries, Event-Driven Architecture's sagas and idempotent consumers, Resilience's backpressure and circuit breakers, and the full observability trio watching over a system where the ordinary act of monitoring it must itself be designed not to become the bottleneck. High-volume transaction processing is, in that sense, less a distinct discipline from everything else in this series than the place where its cumulative lessons about throughput, correctness, and honest reconciliation with reality matter more visibly, and more unforgivingly, than almost anywhere else.

---

*Found this useful? Feel free to star the repo, open an issue with corrections, or share the hot-key contention issue that turned out to matter far more than an aggregate throughput number ever should.*
