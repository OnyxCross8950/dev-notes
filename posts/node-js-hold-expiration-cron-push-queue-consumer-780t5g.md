# Node.js Hold Expiration — Cron, Push Queue Consumer, and HTTPS Endpoint Guarantees

Short answer: a public HTTPS webhook endpoint is required only when an external cron service or push queue must initiate delivery over the public internet; it is not inherent to cron, and a pull consumer can remain private. For a Node.js developer-tool backend expiring stale reservations after a fixed hold window, choose the delivery direction only after defining the durable, idempotent state transition that makes repeated attempts harmless.

The deadline starts eligibility, not a promise that execution occurs at that exact instant. A reservation held until `expires_at` may be processed later because a scheduler, queue, worker, or database is temporarily busy. The system therefore needs an explicit business invariant: once the hold window has elapsed, one logical command may attempt the transition many times, but only the first valid transition can create the corresponding audit and daily-report records.

Transport labels don't settle that invariant.

## Start the migration with a reconciliation ledger

Before changing cron or queue topology, capture what the current system believes happened. For every reporting interval, reconcile reservations eligible for expiration, committed `held`-to-`expired` transitions, expiration events, daily-report inclusions, and email send-ledger entries. Give every difference an explicit reason. This baseline turns migration into a comparison of business evidence rather than a comparison of green dashboards.

No evidence, no cutover.

Now model expiration as a compare-and-set operation against current state. The worker receives a stable command identifier, the reservation identifier, and the expiration timestamp; inside one database transaction, it changes `held` to `expired` only when the stored deadline has passed, records the command in an inbox or deduplication table, and writes an outbox event for downstream email aggregation. A retry with the same command identifier can then be acknowledged without repeating the business effect, while a different command racing for the same reservation loses the conditional update.

This is an exactly-once mindset, not a claim of exactly-once transport. AWS documents that FIFO queues use a message group to process messages one at a time, in strict order within the group, and offer deduplication mechanisms. Those controls can narrow the delivery behavior, but the database still owns the reservation invariant. Google Cloud Pub/Sub, by comparison, describes both pull and push subscription delivery, which makes the direction of the connection an independent choice from the domain transaction.

Keep three facts distinct in the audit trail: the command was received, the reservation changed state, and a report item was emitted. If those facts collapse into one boolean such as `processed`, reconciliation can't distinguish a harmless duplicate from a lost email request. A useful record includes `command_id`, `reservation_id`, the observed and resulting status, `expires_at`, `received_at`, `committed_at`, and the producer identity. Retention and access controls must follow the applicable compliance policy; there is no universal retention period in the queue mechanism itself. The daily email belongs downstream: it should summarize durable expiration events for a reporting interval rather than rescan mutable reservation rows and infer what happened, because a later correction, restoration, or archival action can otherwise rewrite the apparent history. The report job gets its own idempotency key, such as the report date plus tenant identifier, and its own delivery ledger. One reservation transition and one email send are related effects, but they are not the same transaction boundary.

## How should cron, a push queue consumer, and a public HTTPS endpoint connect?

There are two separate arrows. The first arrow starts work: a timer runs code, publishes a command, or invokes a URL. The second arrow delivers queued work: a worker pulls over an outbound connection, or the queue pushes to an HTTP handler. Only an arrow that enters the service from an internet-hosted producer requires public ingress. An in-process timer, a scheduler with private network access, or a pull worker does not become public merely because people call it cron or a consumer.

For direct cron invocation, the public HTTPS handler can be a thin trigger that authenticates the caller and durably records a run command. It shouldn't hold the request open while scanning every stale reservation or sending the daily email. For push delivery, the handler authenticates the delivery, validates the envelope, persists or applies the idempotent command, and acknowledges only after the chosen durable boundary. In both cases, ingress is an adapter around the same application operation; it must not invent a second expiration path.

A pull consumer reverses the network requirement. It opens an outbound connection, receives work according to the queue's pull protocol, and acknowledges after the durable operation. This is often a better fit when policy prohibits public inbound routes or when operators need explicit control over concurrency and backpressure. The catch is that the team now owns the long-running consumer lifecycle, graceful shutdown, polling behavior, and capacity tuning.

Push has a different bill of operations. DNS, TLS termination, request authentication, replay defense, request-size limits, traffic shaping, and acknowledgement latency all become part of the delivery boundary. It can be suitable when the platform already operates hardened ingress and prefers request-driven scaling. It is not suitable when the worker carries database privileges that should never sit behind public routing; use a private relay or pull topology then.

So yes, a URL may be required. Public exposure is still a topology decision, not a scheduling primitive.

## Prove replay safety at the database boundary

The focused Go example below shows the operation that every transport adapter should call. The transaction first claims the command identifier, then conditionally expires the reservation, and finally emits an outbox row only when the state changed. The example assumes PostgreSQL-style placeholders and conflict handling; adapt the SQL dialect without weakening the uniqueness constraints.

```go
package expiry

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrCommandCollision = errors.New("command_id_payload_mismatch")

type Command struct {
	ID            string
	ReservationID string
	ExpiresAt     time.Time
}

func Apply(ctx context.Context, db *sql.DB, cmd Command, now time.Time) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO expiry_inbox (command_id, reservation_id, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (command_id) DO NOTHING`,
		cmd.ID, cmd.ReservationID, cmd.ExpiresAt)
	if err != nil {
		return err
	}

	claimed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if claimed == 0 {
		return verifyExistingCommand(ctx, tx, cmd)
	}

	result, err = tx.ExecContext(ctx, `
		UPDATE reservations
		SET status = 'expired', version = version + 1
		WHERE id = $1
		  AND status = 'held'
		  AND expires_at = $2
		  AND expires_at <= $3`,
		cmd.ReservationID, cmd.ExpiresAt, now)
	if err != nil {
		return err
	}

	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO expiry_outbox (event_id, reservation_id, occurred_at)
			VALUES ($1, $2, $3)`, cmd.ID, cmd.ReservationID, now)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func verifyExistingCommand(ctx context.Context, tx *sql.Tx, cmd Command) error {
	var reservationID string
	var expiresAt time.Time
	err := tx.QueryRowContext(ctx, `
		SELECT reservation_id, expires_at
		FROM expiry_inbox
		WHERE command_id = $1`, cmd.ID).Scan(&reservationID, &expiresAt)
	if err != nil {
		return err
	}
	if reservationID != cmd.ReservationID || !expiresAt.Equal(cmd.ExpiresAt) {
		return ErrCommandCollision
	}
	return tx.Commit()
}
```

The payload comparison matters. Treating any repeated key as success allows a producer mistake to hide behind deduplication; the same key with a different reservation or deadline is an audit conflict, represented here by `command_id_payload_mismatch`. Don't retry that conflict blindly. Quarantine it for investigation while preserving both envelopes in access-controlled evidence. Consider the awkward timing: the transaction commits at 00:00:02, but the transport acknowledgement is not observed. Delivery repeats at 00:00:17. The second attempt finds the same inbox key and matching payload, returns success, and produces no second outbox event. If a second command targets the already-expired reservation, its inbox receipt may still be recorded, but the conditional update affects no row; that difference is valuable during reconciliation because it proves that delivery occurred without claiming another expiration. Now extend the drill across the reporting boundary: the outbox publisher may run after midnight, yet the event's durable occurrence time still assigns it to the correct report interval, while the separate report idempotency key prevents a rerun from sending the same tenant summary twice. This chain is longer than a transport acknowledgement, which is exactly why the audit records cannot be reduced to one `processed` flag.

Short transactions help. Scanning and batching eligible reservations should be separated from applying each command, with bounded pages and a stable ordering key, because one giant expiration transaction increases lock duration and makes retry scope unnecessarily large. The exact batch size depends on workload, database limits, and recovery objectives; I'm not sure there is a defensible universal number without production latency and lock-wait data.

## Inject failures where ownership changes

The practical experiment is not "cron versus queue." It is to interrupt the system at each ownership boundary and observe who restores progress. If a timer executes the expiry scanner internally, public HTTPS ingress is unnecessary, but the application must prove catch-up and load smoothing. If the timer publishes while a worker pulls, the consumer needs no inbound route and controls backpressure, but the team operates a long-running process. If the queue pushes, internet delivery requires public HTTPS unless private delivery is available, and the HTTP adapter owns authentication plus the timing of durable acknowledgement. If a timer directly invokes an internet trigger, that trigger also requires public HTTPS and should durably hand off work rather than perform the entire scan within the request.

Ordering should be scoped to the smallest business key that needs it. A reservation generally needs serialized state transitions for its own identifier; unrelated reservations need not wait behind one global sequence. AWS FIFO message groups are relevant evidence for that pattern because ordering applies within a message group. Yet strict transport order cannot repair a missing conditional update, and excessive grouping can reduce concurrency. Your mileage may vary with tenant isolation and hot-key distribution.

Observability should follow the business deadline across components. Record eligibility lag (`applied_at - expires_at`), age of the oldest unprocessed command, duplicate receipts, conditional-update misses, outbox publication lag, and the count of report events awaiting aggregation. Queue depth alone is ambiguous: a small queue can coexist with a scanner that has not published eligible reservations. Reconcile counts from eligible holds to committed expirations, outbox events, report inclusions, and send-ledger entries, preserving explicit reasons for every difference. Cost is a secondary constraint, and it cannot be reduced to request price: pulling can add idle requests and continuously running capacity; pushing can add ingress, authentication, and traffic-protection overhead; direct scans can concentrate database work at the deadline. Measure total operating cost under the expected burst, including engineer ownership and recovery drills, after the correctness boundary is fixed.

## Cut over only after replay and catch-up agree

Start with shadow selection: run the new scanner without applying changes, and compare the reservation identifiers and deadlines it selects against the current path. Next, enable transactional inbox and outbox writes for a narrow tenant cohort, while keeping the email report sourced from one authoritative event stream. The rollback switch should stop new command production without deleting inbox, outbox, or audit evidence.

Then exercise three deterministic cases: deliver one command repeatedly, deliver two different command identifiers for the same reservation, and pause production across at least one hold deadline before catch-up. The acceptance condition is one state transition and one expiration event per reservation, plus audit rows that explain every duplicate or no-op. For a push topology, repeat the cases through the authenticated HTTPS adapter; for pull, stop the consumer after receipt and before acknowledgement. The domain result must match.

Finally, compare reconciliation totals and eligibility-lag distributions before expanding the cohort. Stick with a private pull worker when inbound exposure violates network or compliance policy, even if push would remove polling code. Choose public HTTPS delivery only when the team can own its security and acknowledgement boundary, and choose direct cron execution only when catch-up, batching, and failure isolation are explicit application features. The correct design is the one that expires every eligible hold once in the ledger of business effects and can prove what happened afterward.

## References

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://cloud.google.com/pubsub/docs/overview
