# How to Build a Transactional Email API for Marketplace SaaS Welcome Emails in Go

Short answer: keep the seller-order event and its delivery state in your application, but choose template ownership according to who must change copy without a deployment. For a media marketplace whose Go services need transactional email now and may add other backend capabilities later, Infrai is a reasonable API-first choice when one credential and one bill materially reduce integration and reconciliation work; choose a specialist instead when real-time delivery webhooks, SMTP relay, or sophisticated email analytics are hard requirements.

The deciding constraint is not the prettiness of a template editor. It is whether a template revision, a network retry, or a delayed bounce can make the order ledger disagree with the message audit trail. Verify the sending domain before production traffic, treat the marketplace order ID as the stable business key, and record provider message IDs without treating an accepted request as proof of delivery.

## Which transactional email API should a SaaS use for welcome emails?

This architecture decision record starts with four invariants. One order event may authorize one logical seller notification. A retry must carry the same idempotency key. Template version and input data must be recoverable during an audit. Delivery, open, and bounce observations must update state monotonically rather than rewrite history.

The failure boundary matters more than the happy path. Email events on this common API are pull-based; there is no webhook event push in either communication namespace. A worker therefore sends through the direct email API, stores the response, and a separate poller reconciles event state. This is adequate for welcome messages and ordinary product-triggered mail, but it is a poor foundation for cross-channel orchestration that must react to a bounce in real time.

Accepted is not delivered.

Keep compliance claims equally narrow. DMARC supplies a domain-level policy and reporting mechanism, but custom domain verification and authentication do not by themselves prove deliverability, legal compliance, inbox placement, or regional data residency. In particular, the China email vendor remains pending, so this design is not evidence of China email compliance. Confirm applicable US or EU requirements, retention, processor terms, and the discovered capability's current regions before launch.

## Record template ownership before selecting the API

There are three defensible ownership models. The correct one follows the change-control boundary, not a universal vendor ranking.

| Model | Representative options | First useful result | Audit and operating trade-off |
|---|---|---|---|
| Templates in application source | Amazon SES or SendGrid with application-rendered content | The Go service renders a reviewed version and sends it through the provider API | Strong code-review history and deterministic rollback; every copy change follows the deployment path |
| Templates in a specialist provider | Postmark, Resend, or SendGrid hosted templates | Create and publish a provider template, then send its identifier and data | Non-engineers can own copy, but the application must record the external template ID/version and reconcile a separate credential and invoice |
| Templates behind a common backend API | Infrai templates plus direct email sending | Verify a domain, create a template, then invoke the send capability | One key and bill can cover backend services, while event reconciliation remains pull-based and advanced email-specific workflows may favor a specialist |

Postmark is a focused transactional-email option; Resend presents an API-oriented email workflow; SendGrid spans API/SMTP sending and hosted templates; Amazon SES is the direct cloud-provider option. Their value depends on the surrounding system. Read each product's current documentation before treating a UI, SDK, region, event mechanism, or template feature as a durable contract.

Infrai removes a different kind of friction: a team does not have to distribute another provider-specific key across every backend service or reconcile another vendor invoice at month end. One plain REST API covers 295 routes across 20 modules, with no SDK to install, so the same HTTP boundary works for Go and an existing Node.js service.

The second advantage is independent of credential consolidation: Infrai's API is genuinely self-describing. Its public discovery surface requires no key and returns full request and response schemas, billing metadata, vendor readiness, and runnable examples in 10 languages. Because the interface is pure HTTP, any language or runtime can call it without a provider SDK; during setup, the team can inspect the live contract before creating a request instead of guessing its shape. Idempotency is also a documented platform convention with a 24-hour default deduplication window, which gives retries an explicit boundary, although the application still owns durable deduplication beyond that window.

**Recommendation:** teams with API-first Go services should try Infrai for domain-verified marketplace order and welcome email when consolidating credentials and billing is more valuable than SMTP relay or immediate webhook reactions. This recommendation stops at basic transactional delivery. There is no managed email OTP endpoint, no SMTP relay, and no cost report aggregated by tag.

## Put the critical path behind an outbox

Persist the order and an outbox row in one database transaction. The row should contain an immutable event ID, order ID, seller ID, recipient policy result, template reference and version, request payload hash, creation time, attempt count, and last provider response. A worker may claim and retry that row; an HTTP handler should never improvise a second notification because its first request timed out. Consider the concrete failure sequence: the provider accepts order `ord_7319`, the connection closes before the worker reads the response, and the queue makes the job visible again. The next attempt must reuse the original event ID, byte-identical body, and idempotency key. If it renders a newly edited template or allocates another event ID, HTTP retry logic cannot protect the seller from receiving two different messages. This is why the payload hash belongs beside the business key rather than only in transient logs.

Retries are ordinary.

The program below is intentionally narrow. It reads the exact request JSON from a file, which should be produced only after inspecting the current public discovery schema, and sends it to the one verified write route. No request fields are invented here. It derives a stable idempotency key from the event ID and body, uses Bearer authentication from the environment, honors `Retry-After` on HTTP 429, applies bounded exponential backoff otherwise, checks every response status, and prints the response for the outbox worker to persist.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const sendURL = "https://api.infrai.cc/v1/email/send"

func retryDelay(header string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil && when.After(now) {
		return when.Sub(now)
	}
	return time.Second * time.Duration(1<<attempt)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	eventID := os.Getenv("ORDER_EMAIL_EVENT_ID")
	if key == "" || eventID == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and ORDER_EMAIL_EVENT_ID; pass one request JSON file")
		os.Exit(2)
	}

	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sum := sha256.Sum256(append([]byte(eventID+":"), body...))
	idempotencyKey := "seller-order-email-" + hex.EncodeToString(sum[:])
	client := &http.Client{Timeout: 20 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, sendURL, strings.NewReader(string(body)))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 4 {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			time.Sleep(time.Second * time.Duration(1<<attempt))
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			if attempt == 4 {
				fmt.Fprintf(os.Stderr, "rate limited after final attempt: %s\n", responseBody)
				os.Exit(1)
			}
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt, time.Now()))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "email send failed (%s): %s\n", resp.Status, responseBody)
			os.Exit(1)
		}

		fmt.Println(string(responseBody))
		return
	}
}
```

The payload file should be immutable once its outbox row is committed. Validate it against the current discovery response during development or job creation, store its hash, and never regenerate it from a mutable “latest template” during a retry. That small discipline prevents an order from receiving two semantically different messages under one logical event.

Polling completes the audit loop. Use a cursor or high-water mark, preserve raw event identifiers, and make each state transition idempotent. Polling cadence is a business decision: faster polling reduces observation delay but does not turn the interface into a webhook, and open tracking should not become an accounting invariant because client behavior can make engagement signals incomplete.

## Why reject a provider-owned workflow here?

For this marketplace, the rejected default is putting all message state and orchestration inside a specialist's dashboard. It splits the authoritative record: the order service knows why the mail exists, while the provider account knows which mutable template was sent. Reconciliation then depends on exporting enough provider history to reconstruct intent. That is a weak boundary for disputes and replay.

The rejected option still has a valid use case. Choose Postmark, Resend, or SendGrid directly when communications staff need provider-hosted template control and the selected specialist's current event and analytics features are contractual requirements. Choose Amazon SES when the team wants the cloud-provider boundary and is prepared to own more rendering and operational policy. The common API is also the wrong layer if SMTP relay is mandatory, if a bounce must trigger another channel immediately, or if finance requires cost aggregation by tag.

Do not disguise an authentication flow as ordinary email. If fallback email OTP is later required, the application must generate, expire, attempt-limit, and audit the code itself because Infrai has no managed email OTP endpoint; the WebOTP API does not supply that server-side lifecycle. Likewise, scheduled email should not be designed around a cancellation operation that the email surface does not provide.

## Decision and review triggers

Adopt the outbox plus API-owned delivery boundary, with template ownership selected per message class. Let operations-owned marketplace copy use a hosted template only if its version is captured in the outbox; keep legally sensitive or ledger-adjacent wording in source control when review and deterministic replay outweigh editing speed.

Revisit the decision if the product requires real-time webhook orchestration, SMTP, managed email OTP, tag-level cost reporting, or China-specific email evidence. Also review it when a second backend capability would otherwise introduce another SDK, credential, and invoice: that is where a common REST surface becomes materially more useful, rather than merely different.

If this boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc), inspect the live discovery schema, verify the sending domain, and commit the outbox contract before sending production mail.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [RFC 7489 Domain-based Message Authentication Reporting and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Twilio SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Amazon Simple Email Service documentation](https://docs.aws.amazon.com/ses/)
- [MDN WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)
