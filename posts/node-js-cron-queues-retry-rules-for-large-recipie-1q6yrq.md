# Node.js Cron Queues: Retry Rules for Large Recipient-List Email Reports

Short answer: treat a scheduled email report as a durable run with per-recipient delivery state; let cron create that run, let queue workers claim small units of work, and let retries advance recorded state rather than replay an opaque batch.

A large mailing list makes a daily report closer to a settlement process than to a loop that happens to call an email API. The relevant question is not how quickly a Node.js process can enumerate addresses. It is whether an operator can later establish which version of a report was intended for which recipient, which sends remain uncertain, and which work can be retried without producing a second observable delivery. The mail transport and the database do not share one transaction, so a design cannot promise literal exactly-once delivery across both systems. It can, however, make every uncertainty explicit and make duplicate attempts harmless where the transport accepts an idempotency key.

Start from the record, then choose the scheduler and queue around it.

## The constraint is a reconcilable delivery record

Create one immutable `report_run` for the reporting cut-off and one `delivery` record for each eligible recipient. A run should point to a pinned report artifact or snapshot, rather than to a query that will be evaluated again during retry. Financial and operational data change after the cut-off; rendering from live state on a later attempt can turn a retry into a different statement with the same run identifier.

The delivery record is the source of truth, not a broker message and not an application log. It needs a stable business key such as `(run_id, recipient_id)`, the snapshot identifier, a state, an attempt count, the next eligible attempt time, and the mail-provider receipt once one exists. Store suppression and consent decisions with enough timing information to explain why a recipient was excluded. For regulated communications, retain the evidence according to the applicable records policy; the article cannot supply a universal retention period because that depends on jurisdiction and the report's legal purpose.

This is the useful distinction: a queue supplies transport and backpressure, while the database supplies auditability. Acknowledging a queue message before the claimed delivery state is durable creates an irrecoverable gap. Recording `sent` before the transport accepts the message creates the opposite gap. Neither gap is repaired by a better cron expression.

## How should a Node.js cron trigger move a daily report email to queue workers for a large recipient list?

Keep the trigger deliberately small. It determines the business date, inserts the uniquely keyed run if it is absent, materializes eligible recipients against the pinned snapshot, and enqueues references to pending delivery rows or bounded chunks. It should not render every message or hold a network connection while it walks a large recipient list. A uniqueness constraint on the run's business key provides the real overlap guard; an in-memory lock only protects one process.

Workers then claim pending rows atomically, render from the pinned artifact, send, persist the result, and acknowledge their queue message last. The following Go sketch shows the ordering. Its interfaces are intentionally generic because the invariant matters more than a particular queue or mail service.

```go
package reportmail

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type Task struct {
	RunID       string
	RecipientID string
}

type Claim struct {
	RunID       string
	RecipientID string
	SnapshotID  string
}

var ErrClaimed = errors.New("delivery already claimed")

type Outbox interface {
	Claim(context.Context, Task) (Claim, error)
	MarkRetryable(context.Context, Claim, error) error
	MarkSent(context.Context, Claim, string) error
}

type Mailer interface {
	Send(context.Context, string, string) (string, error)
}

type Worker struct {
	Outbox Outbox
	Mailer Mailer
	Secret []byte
}

func deliveryKey(secret []byte, runID, recipientID string) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(runID))
	mac.Write([]byte{0})
	mac.Write([]byte(recipientID))
	return hex.EncodeToString(mac.Sum(nil))
}

func (w Worker) Handle(ctx context.Context, task Task, body string) error {
	claim, err := w.Outbox.Claim(ctx, task)
	if errors.Is(err, ErrClaimed) {
		return nil
	}
	if err != nil {
		return err
	}

	receipt, err := w.Mailer.Send(ctx, deliveryKey(w.Secret, claim.RunID, claim.RecipientID), body)
	if err != nil {
		return w.Outbox.MarkRetryable(ctx, claim, err)
	}
	return w.Outbox.MarkSent(ctx, claim, receipt)
}
```

The HMAC makes the key deterministic without exposing the recipient identifier. RFC 2104 defines HMAC as a keyed message-authentication construction; a deployment still needs separate secret management, rotation, and access controls. Do not put the key in ordinary logs. A receipt should be recorded before the message is acknowledged so later reconciliation has a join point for delivery events.

Chunking is an operational choice. One queue message per recipient yields a small failure domain and straightforward retry accounting, but increases broker traffic. A message that represents a bounded page of recipient IDs reduces queue volume, yet the worker must persist each individual delivery before moving on; otherwise one invalid address can force a whole page back through the queue. Keep chunks small enough that their processing time fits comfortably inside the queue's lease or visibility policy.

## What retry state makes duplicate delivery auditable?

Retries need a state machine and a budget. A practical state set is `pending`, `claimed`, `retryable`, `sent`, `suppressed`, and `undeliverable`. The claim must be conditional on a state that is eligible for work, with a lease expiry so a process that stops after claiming does not hold the record forever. A retryable outcome advances `attempt_count` and calculates `next_attempt_at` using capped exponential backoff with jitter. A malformed address or a current suppression record belongs in a terminal state, not in the retry loop.

Short is better here. Count and classify every terminal state.

An ambiguous transport outcome deserves its own treatment. If the worker cannot determine whether the mail service accepted the request, mark the delivery as `unknown` or retain the claim for reconciliation rather than blindly sending again. If the transport documents an idempotency contract, repeat the same deterministic key and use the resulting receipt to resolve the state. If it does not, a human or a provider-side delivery query may be required. Your mileage may vary because that capability is a contract of the selected mail transport, not a property of cron or Node.js.

Priority queues are often proposed when password resets and daily reports share workers. RabbitMQ documents that priority queues use additional resources and that priority applies to messages waiting in the queue; messages already prefetched by consumers are unaffected. Separate queues and consumer budgets are usually easier to reason about when urgent transactional mail must not wait behind a report fan-out. The same rule applies to rate limits: make concurrency a controlled setting, measure queue age and delivery lag, and scale only after the outbox transitions remain correct.

## Roll out by proving the reconciliation path

Begin with a single report type and a small internal cohort. For each run, compare the intended-recipient count with the sum of terminal and outstanding delivery states; this is the reconciliation equation that should drive alerts. Add dashboards for oldest pending delivery, lease expirations, retry count, unknown outcomes, suppression count, and the difference between provider receipts and locally recorded `sent` rows. Exercise the difficult boundaries in tests: trigger overlap, a worker stop after claim, a repeat of an accepted request, and a retry after the reporting data would otherwise have changed.

The catch is operational weight. A per-recipient outbox for a large audience requires partitioning, retention, indexes, and a redrive procedure. It is not suitable when a report goes to a few colleagues, the content has no audit consequence, and an occasional missed run can be safely recreated; a single scheduled process with clear monitoring is then the more proportionate design. Conversely, do not use a daily cron fan-out for recipients who each require a local-time delivery. Store a per-subscription next-run time and have workers claim due subscriptions instead.

The resulting architecture does not depend on a particular scheduler, queue, or Node.js library. Its durable boundary is modest: the trigger creates a uniquely identified run, workers make state transitions idempotently, and operations reconcile the record against the intended audience. That boundary is what turns retries from a source of duplicate email into an accountable part of delivery.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://www.rabbitmq.com/docs/priority
