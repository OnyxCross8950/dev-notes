# Shipment Notifications Under Email Rate Limits: Queue Guarantees for Node.js SaaS

Short answer: for a B2B SaaS system that fans one shipment update out to many subscribers, put a durable outbox beside the shipment transaction, let background workers claim rows at a bounded rate, and define success as a reconciled state transition rather than “the job ran.” Resend, Postmark, and SES belong on the provider side of that boundary; none removes the need for a queue, idempotency, or an audit trail. The cheapest credible design is therefore the one that meets the required delivery guarantee after database, worker, provider, and operational costs are counted—not necessarily the option with the lowest advertised send price.

A shipment event and its email notifications are different records with different lifetimes. The event says what happened to the shipment. Each notification says who should be told, which version they should receive, and what evidence exists that the provider accepted the send. Keeping those facts separate makes retries ordinary and reconciliation possible.

Duplicates are liabilities.

## How should a Node.js SaaS backend queue rate-limited email sending jobs?

Start with the consistency boundary, not with cron or a provider dashboard. When the application commits shipment `SHP-18472` at version `19`, the same database transaction should also record the intended fan-out. If the shipment commits but the notification intent does not, no worker setting can reconstruct the missing obligation reliably; if the intent commits first and the shipment update later fails, subscribers can receive a message about a state that never became authoritative. A transactional outbox closes that gap because the business change and the obligation to notify either commit together or roll back together.

The fan-out should produce one stable identity per subscriber and event version, such as `(shipment_id, shipment_version, subscriber_id, channel)`. Put a unique constraint on that tuple. Replaying the producer then becomes harmless: it asks the database to create the same obligations, and the database rejects duplicate identities before any email API is called. This is an exactly-once mindset applied where it can actually be enforced—inside the local transaction—without pretending that a database and an external email provider share one atomic commit.

Do not hold the shipment request open while workers send mail. A provider limit can turn one update with 30 subscribers into a backlog, and a second customer can create another burst before the first has drained. The request path should persist intent and return; a separate worker pool should claim eligible rows, pace attempts according to the active account limit, and retain enough headroom that ordinary retries do not consume the entire allowance.

PostgreSQL documents `FOR UPDATE ... SKIP LOCKED` as producing an inconsistent view that can be suitable for multiple consumers accessing a queue-like table. That caveat is important. It is useful for claiming work, not for producing a financial-style report of everything outstanding. The following Go fragment shows the claim boundary while deliberately leaving provider-specific pacing outside the database transaction:

```go
package notifications

import (
	"context"
	"database/sql"
	"time"
)

type Job struct {
	ID              string
	ShipmentID      string
	ShipmentVersion int64
	SubscriberID    string
	Attempt         int
}

func Claim(ctx context.Context, db *sql.DB, workerID string, lease time.Duration) (Job, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()

	const query = `
		SELECT id, shipment_id, shipment_version, subscriber_id, attempt_count
		FROM email_outbox
		WHERE state = 'pending'
		  AND available_at <= CURRENT_TIMESTAMP
		ORDER BY available_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`

	var job Job
	if err := tx.QueryRowContext(ctx, query).Scan(
		&job.ID,
		&job.ShipmentID,
		&job.ShipmentVersion,
		&job.SubscriberID,
		&job.Attempt,
	); err != nil {
		return Job{}, err
	}

	const markClaimed = `
		UPDATE email_outbox
		SET state = 'claimed', claimed_by = $1,
		    lease_expires_at = CURRENT_TIMESTAMP + ($2 * INTERVAL '1 millisecond')
		WHERE id = $3`
	if _, err := tx.ExecContext(ctx, markClaimed, workerID, lease.Milliseconds(), job.ID); err != nil {
		return Job{}, err
	}

	return job, tx.Commit()
}
```

The lease is not proof of delivery. It only prevents two healthy workers from intentionally processing the same row at the same moment. If a worker loses contact after submitting the email but before recording acceptance, the system has an ambiguous outcome: retrying may duplicate a message, while suppressing the retry may lose one. A provider-supported idempotency key can resolve that ambiguity when its documented scope and retention cover the retry horizon; otherwise the application must state honestly that the external side is at-least-once and decide which failure is less harmful.

For shipment updates, that policy often varies by message class. A routine “package scanned” notice may tolerate a rare duplicate better than a silent omission. A compliance notice may require different evidence, retention, and manual review. One queue-wide retry rule cannot express both obligations cleanly.

## Delivery guarantees determine the topology

“Background job completed” is an implementation event, not a business guarantee. A useful state machine distinguishes at least `pending`, `claimed`, `provider_accepted`, and terminal review states, with each transition carrying a timestamp, actor, attempt number, and correlation identifier. Logs help operators investigate, but logs alone are a weak audit ledger: retention can differ, fields can drift, and a line can be emitted before the database transaction that it describes commits.

There is no general exactly-once email send across an application database and an unrelated provider. What can be made exact is narrower and still valuable: one shipment version creates one local notification obligation per subscriber; one worker owns a valid lease at a time; one recorded provider acceptance closes that obligation; and every later reconciliation can explain why the record is open or closed. This decomposition avoids a comforting label while giving the team invariants it can test.

The topology follows from those invariants. Use separate logical lanes when transactional shipment notices and bulk digests have different urgency or failure policies. Give every tenant a bounded share if one customer's import can otherwise occupy all workers. Apply a global limiter for the provider account, then a tenant or message-class limiter where fairness requires it. A limiter in one Node.js process is insufficient once replicas scale horizontally, because each replica can believe it owns the full allowance; coordinate permits in a shared store or partition the allowance explicitly among workers.

Backpressure must be visible. Queue depth by itself is misleading because 10,000 newly created jobs and 10,000 jobs that have exhausted ordinary retries imply different action. Track age of the oldest eligible item, claim-to-accept latency, attempt distribution, provider response class, lease recovery count, and the difference between obligations created and obligations closed. Reconciliation should compare durable records, not infer completion from the absence of errors.

The long paragraph matters because the hardest failure occurs between systems: imagine a shipment update creating 240 subscriber obligations, workers consuming the first batch, and the network connection ending before one response is observed. The scheduler may have fired exactly once, the queue may have delivered each row exactly once, and the worker may still lack enough evidence to decide whether a particular recipient was accepted. The correct response is not a blanket retry loop. It is a documented ambiguity policy, a stable idempotency identity where the provider contract supports one, an attempt ledger that never overwrites earlier evidence, and a reconciliation path that can place unresolved items in review without blocking unrelated subscribers. This is also why a dashboard showing “240 jobs processed” cannot satisfy an audit question about 240 notifications.

Keep the scheduler boring. Cron is a time-based trigger, so use it to start a bounded sweep for pending or expired work; do not treat one cron execution as the container for an entire fan-out. The sweep can run more than once because row identity and claims protect the work. It can also run late without changing which shipment versions deserve notification.

## Compare providers, queues, and pricing at the correct boundary

Resend, Postmark, and SES are candidates for accepting email submissions. A database outbox, a managed job service, or a self-hosted queue is a candidate for retaining work and coordinating consumers. Comparing an email provider directly with a background-job system collapses two contracts and usually hides the delivery gap between them.

Use the same evaluation sheet for every candidate, but verify volatile terms against current primary documentation during procurement. The reader's older year qualifier should not anchor a production decision in 2026, especially for account-specific limits and prices.

| Boundary | Evidence to request | Failure question | Cost to include |
| --- | --- | --- | --- |
| Email provider | Current account limit, acceptance semantics, idempotency contract, event retention | Can an ambiguous submission be queried or safely repeated? | Sends, dedicated capacity if required, event storage, support |
| Queue or job runner | Durability, redelivery model, lease behavior, scheduling precision, retention | What happens after a worker dies before acknowledgement? | Operations, requests, retained data, worker runtime |
| Application database | Transaction and locking behavior, backup and restore objectives | Can shipment state and notification intent commit together? | Storage, indexes, write load, maintenance |
| Worker fleet | Shared limiter design, concurrency controls, deployment behavior | Can two replicas exceed the provider allowance? | Compute, observability, on-call effort |

Advertised unit price is only one term. A supposedly cheap provider can be expensive for a team if it demands custom reconciliation, while a managed queue can cost more per operation yet remove enough operational work to be rational; the reverse can also be true for a team that already operates PostgreSQL and has modest throughput. I'm not sure any static public comparison can settle this without the tenant burst distribution, required recovery time, retention obligation, and current account offers. Those four inputs would resolve most of the uncertainty.

The catch is that the outbox approach is not suitable when the database cannot absorb the extra writes and polling pattern, or when workflows require long-running coordination, human approvals, or multi-step compensation. In those cases, use a durable workflow engine or a queue designed for that lifecycle, while keeping the shipment event and notification identity explicit. Conversely, stick with a PostgreSQL outbox when atomic intent capture is the dominant constraint, the team already operates the database well, and the measured load fits its capacity. This is a boundary decision, not a product ranking.

## Roll out with reconciliation before increasing throughput

Begin with one message class and shadow the fan-out: create obligations, claim them, and record what would have been sent without contacting recipients. Compare the expected subscriber set with the generated obligations, including subscription changes that race with shipment updates, then test worker termination before submission, during an ambiguous submission, and after recorded acceptance. A deployment test should also prove that two worker versions preserve the same state-transition rules while they overlap.

Next, enable a small production slice with conservative pacing derived from the chosen provider's current documented limit. Alerts should fire on oldest-item age and unexplained state transitions, not merely process health. Run reconciliation on a schedule independent of the normal workers so a defect in the consumption path cannot also silence its checker.

Only then increase concurrency. Slowly.

Before declaring the migration complete, demonstrate three queries: which subscribers were owed a notification for shipment version `19`, which attempts were made for each obligation, and which evidence closed each one. If any answer depends on searching ephemeral logs or trusting that cron ran, the system has a scheduler but not a defensible delivery guarantee.

## References

- https://en.wikipedia.org/wiki/Cron
- https://www.postgresql.org/docs/current/sql-select.html
