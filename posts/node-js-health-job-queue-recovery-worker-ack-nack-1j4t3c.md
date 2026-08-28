# Node.js Health Job Queue Recovery: Worker Ack, Nack, Retry, Dead Letters, Idempotency

A healthtech worker pool that is already constrained by a downstream rate limit cannot recover safely by consuming faster; additional concurrency merely creates more rejected work and makes the queue's visible depth a poor measure of progress. **Short answer:** create a bounded queue, keep delivery at least once, acknowledge only after the durable business effect and its audit record commit, retry HTTP 429 responses according to `Retry-After`, and move exhausted or invalid work to a dead-letter queue for controlled redrive.

This architecture decision record treats operational recovery as the primary constraint. The concrete workload is a pool that dispatches patient-notification jobs to a rate-limited endpoint, although the same boundaries apply to eligibility refreshes and document-processing callbacks. The goal is not the impossible promise that a message will be delivered once. The goal is an exactly-once business effect, demonstrated by a stable idempotency key, a transaction record, and reconciliation evidence even when the worker exits between commit and acknowledgement.

Recovery is part of correctness.

## How should a Node.js background job queue worker consume, ack, nack, and retry?

The producer should create a small job envelope containing a schema version, a job identifier, a subject reference, and the operation to perform. It shouldn't put an entire clinical record in the message. A reference limits duplicated sensitive data, while the schema version lets a worker reject an incompatible shape deliberately rather than guessing. The business identifier must survive every retry and redrive; a broker-generated delivery identifier isn't an adequate substitute because a newly published copy may receive a different delivery identifier.

Consumption begins only when the worker has capacity under both limits: its local concurrency budget and the downstream service's current admission budget. Once a message is reserved, the worker validates the envelope, starts an attempt record, and executes the effect under the stable idempotency key. It then commits the effect and audit evidence before sending `ack`. If the process stops after the commit but before the acknowledgement, the broker may deliver the message again; the next transaction finds the completed idempotency record, records a duplicate attempt, and acknowledges without repeating the effect.

That's the critical gap.

An HTTP 429 response means the client has sent too many requests in a period, and the response may include `Retry-After` to indicate how long to wait. The worker should preserve the message and schedule its next eligibility time from that value when present. When the header is absent, a locally defined backoff with jitter is an operational policy rather than a fact inferred from the response. Either path should release scarce worker capacity instead of sleeping while holding a slot, because a pool full of sleeping jobs cannot drain unrelated work that has become eligible.

Use `nack` with requeue for a retryable attempt only when requeueing cannot cause an immediate hot loop. Consumer acknowledgement and publisher confirmation solve different problems: an acknowledgement tells the broker what happened to a delivery, while a publisher confirmation concerns whether the broker accepted a publication. RabbitMQ's documentation also distinguishes positive acknowledgement from negative acknowledgement and rejection, including whether a rejected delivery is requeued. The application still owns the classification policy.

Invalid schema, an impossible operation, or a job whose retry budget is exhausted belongs on the dead-letter path. Preserve the original job ID, the terminal reason, the attempt count, and the last eligible time. Don't automatically replay that queue on a timer. An operator should first correct the cause or explicitly accept that the same terminal result will recur.

## Invariants and failure boundaries

Four invariants make the system recoverable. First, every logical operation has one stable idempotency key, preferably derived from a producer-owned job ID plus an operation version rather than mutable patient data. Second, the effect and the completed-idempotency record commit atomically when they share a database. Third, acknowledgement happens after that commit. Fourth, every state transition emits enough audit evidence to answer who or what attempted the job, when it became eligible, why it was retried, and which durable record proves completion.

The database constraint is the arbiter. Two workers can receive equivalent work during a redrive or producer race, so an in-memory set and a check-then-write query are insufficient. A unique constraint on the idempotency key, combined with a transaction that either creates the effect and completion record together or observes the existing completion, turns the race into a deterministic result. The attempt ledger can remain append-only while the current job state is updated, giving reconciliation both a history and a compact answer.

An external side effect changes the boundary. If the worker must call another system and then update its own database, no local transaction can atomically cover both resources. The receiving system should honor the same stable idempotency key; if it cannot, the design cannot honestly claim an exactly-once effect. The defensible alternatives are to tolerate and reconcile duplicates, introduce a transactional outbox before the call, or move the effect behind an interface that can deduplicate. Which alternative is acceptable depends on the clinical and compliance impact of a duplicate, not on queue throughput.

The audit trail also has a data-minimization boundary. Store opaque subject references and operational metadata unless the applicable access, retention, and disclosure controls explicitly permit more. Logs are part of the system of record for an investigation, but they shouldn't become a second uncontrolled copy of the payload. I've found the useful design question is narrower than “did the worker run?”: can an authorized reviewer connect the accepted job, every attempt, the committed effect, and the final acknowledgement without reconstructing intent from free-form log lines?

I'm not sure a static retry ceiling can be chosen from architecture alone. The answer requires the endpoint's recovery behavior, the job's useful lifetime, and the harm of delayed versus duplicated execution. Those inputs should resolve the uncertainty; copying a fashionable attempt count won't.

## Option comparison for operational recovery

The queue mechanism matters less than the failure contract around it. This table compares architectural shapes without ranking products:

| Shape | Recovery strength | Failure boundary | Suitable when | Not suitable when |
| --- | --- | --- | --- | --- |
| Broker queue with explicit ack/nack | Redelivery is visible and worker ownership is clear | The application must implement idempotency, retry timing, and audit correlation | Jobs are independent and one durable effect defines completion | Work requires durable multi-step compensation or joins |
| Database-backed queue | Claiming work and recording a local effect can share transaction semantics | Polling, contention, and retention affect the primary database | The workload is moderate and the database is already the correctness boundary | Queue traffic would compete with clinical transactions for the same capacity |
| Durable workflow runtime | Step history can represent waits and recovery across a sequence | The workflow model and runtime become part of every operation | Jobs span several dependent actions with explicit compensation | Each job is one short, independently idempotent effect |
| Direct scheduled HTTP invocation | Few moving parts for a small trigger | Rate limiting and downtime meet the scheduler directly, with no independent backlog | The action is quick, safe to repeat, and missing its window has little impact | Operators need controlled draining, dead-letter inspection, or redrive |

For the patient-notification pool, a broker queue or database-backed queue is the smaller adequate abstraction if each job is one idempotent dispatch. The catch is that neither shape supplies end-to-end exactly-once behavior. A durable workflow is justified when consent evaluation, content generation, dispatch, and delivery recording form a stateful sequence with compensating decisions. Direct invocation is the rejected option here because it couples schedule pressure directly to the rate-limited endpoint and leaves no separate backlog to pause, inspect, or drain.

Yet direct invocation has a valid use case: a low-consequence cache refresh that can be skipped and recomputed later. Architecture boundaries should preserve that distinction rather than turning the most demanding health workflow into a universal template.

## Critical path in Go

The interfaces below make the ordering explicit and map directly to the callbacks exposed by a Node.js consumer. `Store.ApplyOnce` represents one database transaction: it inserts the idempotency record under a unique key, applies the business change if the key is new, and appends audit evidence. `Queue.Nack` schedules rather than immediately requeues a rate-limited message, while `DeadLetter` records a terminal classification. Broker-specific queue creation and transport configuration remain outside the business handler, where they can be tested independently.

```go
package worker

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"
)

type Job struct {
	ID             string
	SubjectRef     string
	Operation      string
	Attempt        int
	Receipt        string
	FirstSeenAt    time.Time
}

type Queue interface {
	Consume(context.Context) (Job, error)
	Ack(context.Context, string) error
	Nack(context.Context, string, time.Time) error
	DeadLetter(context.Context, Job, string) error
}

type Store interface {
	// ApplyOnce commits the effect, idempotency key, and audit row atomically.
	ApplyOnce(context.Context, Job) (alreadyApplied bool, err error)
}

type RateLimitError struct {
	RetryAfter string
}

func (e *RateLimitError) Error() string { return "downstream rate limit" }

func retryTime(now time.Time, value string, attempt int) time.Time {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at
	}
	delay := time.Duration(1<<min(attempt, 6)) * time.Second
	return now.Add(delay)
}

func Handle(ctx context.Context, q Queue, store Store, job Job, now time.Time) error {
	if job.ID == "" || job.SubjectRef == "" || job.Operation == "" {
		return q.DeadLetter(ctx, job, "invalid envelope")
	}

	alreadyApplied, err := store.ApplyOnce(ctx, job)
	if err == nil {
		// A duplicate still receives an ack because the durable effect exists.
		_ = alreadyApplied
		return q.Ack(ctx, job.Receipt)
	}

	var limited *RateLimitError
	if errors.As(err, &limited) && job.Attempt < 8 {
		return q.Nack(ctx, job.Receipt, retryTime(now, limited.RetryAfter, job.Attempt))
	}

	return q.DeadLetter(ctx, job, "terminal or retry budget exhausted")
}
```

The example intentionally avoids a timer inside `Handle`; returning the delivery with a future eligibility time frees the worker slot. Production code also needs cancellation, bounded fetch size, graceful shutdown that stops new consumption before waiting for active handlers, metrics for queue age and outcome, and a reconciliation query that compares accepted jobs with completed effects. Queue depth alone is insufficient during recovery: age of the oldest eligible job, the distribution of attempts, 429 frequency, and dead-letter growth reveal whether the pool is draining or merely cycling work.

Testing should force the dangerous boundaries. Terminate a worker after `ApplyOnce` commits but before `Ack`, deliver the same job concurrently to two handlers, omit `Retry-After`, provide both supported header forms, exhaust the retry budget, and redrive a corrected dead-letter entry with the original idempotency key. The pass condition is not “the handler returned no error.” It is one durable effect, a complete attempt history, and an acknowledgement only after evidence of that effect exists.

Deployment uses the same logic. Reduce admission first, stop fetching new jobs, allow bounded in-flight work to finish, and then replace workers. During a backlog drain, increase concurrency only while the observed downstream allowance supports it; otherwise the pool amplifies 429 responses. This design is deliberately conservative — it trades peak drain speed for a recovery procedure that can be paused, inspected, reconciled, and resumed without inventing a new identity for the same job.

## Sources

- https://www.rabbitmq.com/docs/confirms
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429
