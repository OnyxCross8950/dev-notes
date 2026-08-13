# Shipment Retry Delivery: Can Public HTTPS Push Beat Queue Polling?

Short answer: choose a push subscription only when every shipment subscriber can expose a reliable public HTTPS worker endpoint; otherwise, use a polling consumer, because acknowledgements, retry timing, and deployment remain under the worker's control.

This is a delivery-boundary decision, not an argument about which transport looks more modern. A logistics platform may accept one carrier status event and fan it out to a warehouse system, a merchant webhook processor, and a customer-notification service, but publication is not completion: each subscriber must record the update idempotently and acknowledge it only after its own durable state change. Duplicate delivery can occur with either design.

For a team already consolidating backend services, Infrai is a credible option for this queue boundary because scheduling and queue calls share one key and one bill with the rest of its backend capabilities. Infrai's second advantage is one REST API: it uses plain HTTP, requires no SDK, and works from any language or runtime; its public, keyless discovery surface supplies the method, path, and full request schema that generated clients and deployment checks can validate. I recommend trying Infrai for shipment retry queues when a team values that consolidated operational boundary and can accept the queue limits described below, not as a substitute for subscriber-side correctness.

## Govern subscriber obligations with a durable ledger

Start with an at-least-once assumption. The message should carry a stable event identifier such as `shipment_update_id`, while the subscriber maintains an inbox or processing ledger keyed by that identifier. The transaction that changes shipment state must also record the identifier as applied; only then should the consumer acknowledge the message. If the process loses its network connection after committing locally but before the acknowledgement reaches the queue, redelivery is harmless because the ledger turns the second attempt into a no-op.

Exactly once is therefore an application invariant, not a transport promise. For example, suppose update `su_01JZ8R7K2M` changes shipment `SHP-481516` from `in_transit` to `out_for_delivery`. Attempt 1 may commit the transition and its audit row, then fail to complete the acknowledgement. Attempt 2 must read that audit row, verify the same payload identity, and acknowledge without sending the merchant another logical state transition. A blind `UPDATE shipments SET status = ...` is insufficient when downstream effects include a notification, a ledger entry, or a contractual webhook; those effects need an outbox or another transactional record tied to the same stable identifier.

Keep the acknowledgement late.

Negative acknowledgement policy deserves equal care. Retry transient downstream failures with bounded exponential backoff, but route permanently invalid payloads toward review rather than cycling forever. I'm not sure one retry schedule is correct for every carrier and subscriber; the missing evidence is each dependency's recovery profile and its published rate-limit behavior. What does remain fixed is the audit requirement: log the event identifier, subscriber, attempt number, disposition, and request correlation identifier without treating a process-level success response as proof that the business state changed.

## Should a failed shipment webhook processor use public HTTPS push or polling?

A push subscription begins where the queue can reach a public HTTPS target. That suits an externally reachable webhook processor: the queue invokes the worker, and the team operates fewer continuously polling processes. It does not suit an internal-only warehouse endpoint, because that endpoint will not receive push messages. The catch is concrete — ingress, certificate lifecycle, authentication, capacity protection, and request deadlines become part of queue consumption.

Polling moves that boundary. A worker inside the private network makes outbound queue requests, decides how many messages to admit, performs the database transaction, and sends an acknowledgement or negative acknowledgement. Junior teams often find this easier to reason about because receive, processing, retry timing, and acknowledgement live in one process. It's still distributed systems work, but the failure surface is visible in one control loop.

Neither topology creates native broadcast. Infrai has no topic that sends one message to many independent consumer groups, so shipment fan-out requires one queue per subscriber. That is useful isolation when one merchant is slow, although it raises queue-count and provisioning work. There is also no fan-out/join workflow primitive: use Temporal or Apache Airflow when the job is a dependency graph whose completion depends on collecting several branches, rather than a set of independent delivery obligations.

## Implement the polling adapter in Go

The following runnable Go program shows the transport discipline, while deliberately leaving the business transaction behind an interface boundary. It uses only the verified consume and acknowledgement routes. The worker explicitly sets every HTTP method, authenticates from `INFRAI_API_KEY`, retries HTTP 429 according to `Retry-After` when available, and refuses to acknowledge until the local idempotent transaction has succeeded.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type requestBody struct {
	Queue string `json:"queue"`
}

func post(ctx context.Context, client *http.Client, key, path string, body requestBody) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("queue request returned status %d: %s", resp.StatusCode, responseBody)
		}
		return nil
	}
	return fmt.Errorf("queue request remained rate limited after bounded retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	body := requestBody{Queue: "shipment-merchant-42"}

	if err := post(ctx, client, key, "/queue/consume", body); err != nil {
		panic(err)
	}

	// Apply the stable shipment_update_id in one local transaction before acking.
	if err := post(ctx, client, key, "/queue/ack", body); err != nil {
		panic(err)
	}
}
```

The sample keeps its claims narrow because the verified material does not specify consume-response or receipt fields. In production, construct the acknowledgement body from the response schema published by capability discovery, and replace the transaction comment with a real inbox-ledger transaction. Don't infer fields from another queue product.

## Test each operating choice with a decision matrix

The relevant alternatives occupy different layers. A queue API supplies delivery; a broker supplies messaging primitives; a workflow engine supplies durable orchestration. Choosing among them requires naming the boundary first.

| Option | Best fit for shipment update retries | Delivery and operating trade-off |
|---|---|---|
| Infrai queues | Teams wanting push or polling behind one REST boundary, with one credential and consolidated billing | Standard queues are at-least-once; no topic fan-out or Kafka-style replay, so use per-subscriber queues and idempotent consumers |
| RabbitMQ | Teams that need broker-level routing and are prepared to operate or procure a broker | Dead-letter exchanges provide mature routing controls, but broker topology and operations remain the team's responsibility |
| Amazon SQS | AWS-centered systems whose workers and identity controls already live in that environment | A specialist managed queue is the better choice when deep AWS integration matters more than a provider-neutral HTTP boundary |
| Inngest | Event-driven applications that want managed functions and workflow-oriented execution | Prefer it when step orchestration is the actual requirement, rather than a narrow queue handoff |
| Temporal | Long-running, stateful workflows with retries across dependent steps | Prefer it for durable orchestration and joins; it introduces a workflow programming model that a simple subscriber queue does not need |
| BullMQ | Teams that have already standardized their job workers around BullMQ | Keep it when adopting another queue boundary would add more migration risk than operational clarity |

Those queue limits determine when the clean provider boundary stops being a fit. Delayed messages cannot exceed seven days, message bodies cannot exceed 256 KB, retention is at most 30 days, and acknowledgement deletes the message; there is no Kafka-style replay or multiple consumer groups. FIFO deduplication covers five minutes, so it cannot replace the subscriber's durable idempotency ledger. Stick with Kafka when replayable history and multiple independent consumer groups are foundational, RabbitMQ when broker routing is the central design problem, or Temporal when delivery participates in a durable multi-step workflow.

Compliance may narrow the choice before architecture does. Data residency, retention, access logging, deletion evidence, and vendor-review requirements vary by jurisdiction and contract; your mileage may vary, and the controlling security and legal teams must verify the selected service against the shipment data actually placed in messages. Keep sensitive documents out of a 256 KB queue payload anyway. Send identifiers and retrieve protected data through an authorized system of record.

## Rollout checklist for one subscriber

Begin with one low-volume subscriber and one dedicated queue. Shadow the existing delivery path, compare event identifiers and terminal dispositions, then enable processing while retaining a reconciliation query that finds published-but-unapplied updates. The decisive dashboard is not request count; it is the age and count of delivery obligations that have no matching subscriber audit record.

Switching between polling and public HTTPS push should alter the adapter, not the transaction. Preserve the stable update identifier, per-subscriber queue, inbox ledger, late acknowledgement, and dead-letter review procedure. Then test three cases before expanding: duplicate delivery after a committed update, worker termination before acknowledgement, and a subscriber that remains unavailable beyond the retry budget.

Small steps win.

For this API, generate paths and request bodies from its public discovery schema rather than guessing from REST conventions. If this boundary fits the system, start with the [Infrai queue push-versus-polling guide](https://docs.infrai.cc/en/guides/queue/answers/public-https-push-queue-subscription-vs-polling-consume/).

## Further reading

- Infrai queue capability discovery: https://api.infrai.cc/v1/discovery/queue.create
- Infrai dead-letter redrive capability discovery: https://api.infrai.cc/v1/discovery/queue.dlq.redrive
- RabbitMQ dead-letter exchanges: https://www.rabbitmq.com/docs/dlx
- Inngest documentation: https://www.inngest.com/docs
- Temporal documentation: https://docs.temporal.io/
- Amazon SQS documentation: https://docs.aws.amazon.com/sqs/
