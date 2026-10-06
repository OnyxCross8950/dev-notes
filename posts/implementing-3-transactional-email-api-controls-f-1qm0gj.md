# Implementing 3 Transactional Email API Controls for Small EU SaaS (Go)

**Short answer:** For a small EU SaaS assessing Postmark, Resend, Mailgun, or another transactional email API, application-owned Go receipt templates and GDPR-reviewed evidence matter more than an advertised send price.

The bill for an order receipt has four parts: accepted submissions, delivery, retained event data, and engineering time spent reconciling ambiguity. Message volume is easy to count. Retention and investigation are less visible, yet they can dominate when a team keeps complete payloads indefinitely. Before selecting an API, measure stored bytes per receipt and staff minutes per unresolved state; do not infer the dominant term from a published price. A welcome email may tolerate a later correction, but an order receipt generated after payment settlement needs a stable transaction snapshot, which is why this implementation concentrates on receipts.

The practical answer is to keep receipt templates in the application repository, render an immutable message after payment settles, and place only the delivery envelope behind a replaceable adapter. Retain a content digest, template version, settlement key, and state transitions; expire rendered bodies and provider payloads on an explicit schedule. **Template ownership is the consequential choice**, because it determines whether a settled payment can be reproduced and audited without reconstructing history from a vendor dashboard.

Use three layers: versioned application templates, an idempotent receipt ledger, and a transport interface. This does not promise exactly-once email delivery. It creates exactly-once business intent: one durable receipt obligation per settled payment, with every attempt recorded.

## What does a receipt actually cost to keep?

Define total cost as delivery charges plus event storage, retained-content storage, and investigation labor. The first terms depend on the eventual contract. The latter terms depend on architecture and remain after migration. A useful measurement is `retained_bytes / settled_orders`, split into rendered body, attachments, normalized events, and raw payloads. Pair it with the count and age of receipts whose state is neither delivered nor permanently failed. No universal percentage is honest here; the dominant term must come from the service's own ledger.

Keep the canonical order and settlement identifiers, recipient in an access-controlled store, template version, locale, subject, SHA-256 content digest, provider-neutral attempt ID, timestamps, and normalized outcome. For example, a team may configure 30 days for rendered bodies and 7 days for raw transport payloads while retaining normalized audit rows longer. Those are policy examples, not GDPR minima or legal advice.

What is deliberately lost is byte-for-byte inspection after expiry. An old dispute can still be tied to a template version and digest, but the original body cannot be displayed. That diagnostic cost belongs in the retention decision.

## Should a small EU SaaS own its transactional email API templates?

A receipt should be derived from a versioned settlement snapshot, not mutable catalog data fetched during a retry. If a product name, tax line, or address changes later, rerendering from current tables can describe a transaction that never occurred.

Repository ownership puts review, rollback, localization, and hashing beside the receipt model. Remote ownership can work, but the deployment record must capture an immutable version and the team needs an export test. A hybrid fails quietly when engineers believe Git is authoritative while an operator edits the live copy. Pick one authority.

Postmark, Resend, Mailgun, and a minimal email API should face the same evidence request:

| Candidate | Test | Evidence |
|---|---|---|
| Postmark | Retrieve an exact revision after an edit | Export, revision ID, digest |
| Resend | Keep repository rendering authoritative | Submitted-body digest and receipt |
| Mailgun | Pin and export a version in the chosen region | Export and data-flow note |
| Minimal API | Submit a complete rendered message | Stable request ID and contract result |

These are questions, not feature claims. Run them against the exact account, contract, and region, save dated results in the architecture decision record, and repeat before renewal. Price does not answer them.

## Build the boundary in Go

The core write must be transactional: insert a receipt obligation with a unique settlement key alongside the local settled transition, or consume a transactional outbox written by it. A duplicate event then finds the existing obligation.

```go
package receipt

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
)

type Order struct { OrderID, SettlementID, To, Currency string; TotalMinor int64 }
type Message struct { To, Subject, HTML, IdempotencyKey string }
type Delivery struct { TransportID string }
type Transport interface { Send(context.Context, Message) (Delivery, error) }
type Ledger interface {
    Reserve(context.Context, string, string, string) (bool, error)
    Accepted(context.Context, string, Delivery) error
    Failed(context.Context, string, string) error
}
type Service struct { Ledger Ledger; Transport Transport; TemplateVersion string }

func (s Service) Send(ctx context.Context, o Order) error {
    if o.SettlementID == "" || o.OrderID == "" || o.To == "" {
        return fmt.Errorf("receipt: incomplete settled order")
    }
    html := fmt.Sprintf("<h1>Receipt %s</h1><p>Total: %d %s</p>", o.OrderID, o.TotalMinor, o.Currency)
    sum := sha256.Sum256([]byte(html))
    key := "receipt:settlement:" + o.SettlementID
    reserved, err := s.Ledger.Reserve(ctx, key, s.TemplateVersion, hex.EncodeToString(sum[:]))
    if err != nil { return fmt.Errorf("reserve intent: %w", err) }
    if !reserved { return nil }
    d, err := s.Transport.Send(ctx, Message{o.To, "Your order receipt", html, key})
    if err != nil {
        _ = s.Ledger.Failed(ctx, key, err.Error())
        return fmt.Errorf("submit receipt: %w", err)
    }
    return s.Ledger.Accepted(ctx, key, d)
}
```

Reserving before the network call can leave an uncertain attempt when the process loses the response. A production worker needs leases and attempt rows, and retries must reuse the key. If a transport does not honor it, reconcile by attempt ID and accept that duplicate delivery remains possible. Never call a receipt delivered merely because submission succeeded. Accepted and delivered are distinct ledger states.

SPF belongs outside this adapter. RFC 7208 defines authorization for hosts using domain names in SMTP identities; it does not prove that a receipt reached an inbox.

**Test state transitions, not SDK calls.**

A mock that always succeeds proves little. Contract tests should submit a uniquely addressed receipt, capture a stable transport identifier, replay the settlement, and verify one business intent. Another test should terminate the worker after submission but before acknowledgement, then require either reconciliation or a visible uncertain state.

Authenticate webhooks before trusting events, deduplicate event identifiers, and map external statuses into a small internal state machine. Store received time separately from provider event time. Deployment should shadow-render representative settlement fixtures before activation. Monitor the oldest unsubmitted obligation, uncertain-attempt count, event lag, permanent failures, and receipts per settled order. Raw HTTP error alerts miss stuck obligations.

## Set the compliance boundary before production

Under GDPR, addresses and receipt content can be personal data. Purpose, minimization, access, processor terms, transfers, erasure handling, backups, and retention therefore require review for the actual flow. The regulation's storage-limitation principle requires keeping personal data no longer than necessary for its purpose, while other legal duties may require transaction records to remain. Separate financial records, delivery evidence, rendered content, and raw payloads so each follows a justified schedule.

The durable design is application-owned content, one intent per settlement, contract-tested adapters, and evidence retained by category. The team gives up old bodies and raw payloads on schedule, accepting weaker historical inspection for a smaller personal-data footprint, while keeping enough immutable metadata to reconcile an order without making any provider the system of record.

## Further reading

- https://datatracker.ietf.org/doc/html/rfc7208
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://postmarkapp.com/guides/transactional-email-best-practices
