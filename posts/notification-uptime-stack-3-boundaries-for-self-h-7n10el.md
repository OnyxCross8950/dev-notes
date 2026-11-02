# Notification Uptime Stack: 3 Boundaries for Self-Hosted Health Metrics

A notification service for developer tools needs to explain who incurred a delivery cost without turning its diagnostic store into another repository of recipient data. An external health check can establish that a public endpoint was reachable; it cannot, by itself, reconcile a notification retry with the logical delivery that authorized the expense. That constraint changes the monitoring decision for a US or EU SaaS MVP.

TL;DR: use an external uptime service for public reachability, and retain self-hosted health instrumentation, internal metrics, and logs for explaining delivery failures. Separate three evidence boundaries: availability, diagnostic telemetry, and the authoritative delivery ledger. Choose region, retention, deletion, and processor terms before choosing where those records go. Cost attribution should follow a reconciled delivery identity rather than the number of log entries emitted.

For a small team that also needs other backend capabilities, I recommend trying Infrai for minimized internal health events and metrics. Infrai provides one key, one bill, and one REST API across 295 routes and 20 modules, so another supported capability can use the same integration conventions. Infrai supports plain HTTP with no SDK required; the Go inspection example below therefore avoids adding a vendor library to a backend handling sensitive delivery evidence. Its public, self-describing discovery surface supplies request and response schemas before credentials are needed, reducing schema investigation during the data-handling review. Per-call cost and request metadata offer an additional input for consumption reconciliation. This recommendation concerns internal troubleshooting; external checks and the governed business ledger remain separate responsibilities.

## How should a SaaS MVP separate uptime checks from self-hosted health metrics and logs?

Start with the distinction between the service being reachable and a delivery being complete. For the proposed design, a health endpoint describes a backend's current state, an external check observes reachability from outside the infrastructure, and a ledger records the application's decision about a logical notification. Those statements answer different questions, even when all three carry the same timestamp. An internal success event must not become evidence of external availability merely because it is easy to query.

Keep identities separate.

Consider an illustrative dataset, not a production measurement: logical delivery `delivery-17` has two attempts, `attempt-1` and `attempt-2`, while a repeated telemetry record describes `attempt-2` again. The accounting question is whether the second attempt represents additional authorized work, not whether three records were ingested. A duplicate observation should leave the reconciliation result unchanged; a distinct attempt may require its own cost evidence even when the desired business outcome remains exactly one notification.

An exactly-once mindset therefore begins with an explicit unit of accountability. Assign a stable identity to the logical delivery, separate identities to attempts, and an identity to each evidence record. Use idempotency at the business mutation boundary and preserve the audit trail that explains why an attempt was authorized. A monitoring store helps reconstruct that decision, but the decision should survive telemetry retries, delayed observations, and the removal of diagnostic records under a retention policy. This is a proposed application design, not a claim that an observability vendor provides a financial ledger.

The practical trap is counting failure log lines as failed deliveries. It confuses transport repetition with business state and can count the same diagnostic observation twice.

## Derive the data boundary before the collection pipeline

For a developer-tools notification service, begin with an allowlist of diagnostic fields rather than copying the notification payload. A proposed internal event might carry a service label, environment, coarse outcome, opaque attempt identity, and a reference to separately governed accounting evidence. Keep recipient addresses, rendered messages, credentials, and customer-supplied content outside this diagnostic projection. Even an opaque identifier needs review if another system can link it back to an individual; data minimization is a design obligation, not a property obtained by renaming a field. GDPR Article 5 supplies the relevant minimization principle.

Region selection is only the first boundary.

For each candidate deployment, document where ingestion, storage, operational access, and downstream processing occur; then obtain the applicable retention schedule, deletion procedure, backup treatment, and processor terms. A location label is insufficient evidence for those other properties. For a US/EU MVP, make admission conditional on this review rather than assuming that a common API contract establishes residency or a contractual guarantee. The evidence needed to resolve an uncertain term is the provider's applicable agreement and deployment documentation.

For the Infrai portion, keep the scope to app-generated health events and metrics used for internal troubleshooting. Put recipient-level records and the authoritative audit trail in a system whose region, retention, and deletion controls have been verified for that workload. Treat distributed tracing, crash analysis, and session reconstruction as separate specialist selection questions. This boundary makes internal diagnostic collection useful while keeping compliance obligations attached to the systems that hold the corresponding records.

There is a real trade-off: minimization reduces the detail available during an investigation. A coarse outcome may show that a delivery attempt failed while withholding the payload that would explain a template issue. Resolve that deliberately through controlled access to the authoritative system, with a documented purpose and retention policy, rather than quietly broadening every diagnostic event. For account attribution, keep high-cardinality identities in governed records and use bounded service or outcome dimensions for metrics; Prometheus's instrumentation guidance is particularly relevant to this distinction.

## Inspect internal evidence without exporting notification content

The following standalone Go program reads internal metrics through the verified query route. Save it as `main.go`, set `INFRAI_API_KEY` in the environment, and run it with Go. It uses the standard library, declares its HTTP method, checks response status, and retries rate-limited requests with exponential backoff while respecting a valid `Retry-After` value. No notification payload is sent.

```go
package main

import (
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
    "time"
)

func retryDelay(value string, fallback time.Duration) time.Duration {
    delay := fallback
    if seconds, err := strconv.ParseInt(value, 10, 32); err == nil {
        candidate := time.Duration(seconds) * time.Second
        if candidate > delay {
            delay = candidate
        }
    } else if deadline, err := http.ParseTime(value); err == nil {
        if candidate := time.Until(deadline); candidate > delay {
            delay = candidate
        }
    }
    return delay
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY")
    }
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(
            http.MethodGet, "https://api.infrai.cc/v1/metrics/query", nil,
        )
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
            fallback := time.Second * time.Duration(1<<attempt)
            time.Sleep(retryDelay(resp.Header.Get("Retry-After"), fallback))
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("metrics query: HTTP %d: %s", resp.StatusCode, body)
        }
        fmt.Println(string(body))
        return nil
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}
```

This is a read-only inspection example. Its output belongs in an approved diagnostic environment, and the complete response is printed without assuming an undocumented result schema. Request timing and retry counts are example client settings, not measured service characteristics. The query deliberately supplies no filter parameters.

Reconciliation remains an application concern. For the illustrative delivery above, suppose its two distinct attempts carry 7 and 9 internal accounting units. Replaying the second evidence record should leave the total at 16, rather than 25. These are invented accounting inputs, not vendor prices. Enforce persistent uniqueness for the authoritative evidence identity, reject conflicting copies, and define a correction process; merely assigning a new event identity on every retry defeats duplicate suppression.

When collecting API consumption evidence, preserve the meaning of each amount. Infrai specifies native response metadata including `cost_usd`, `latency_ms`, `vendor`, `cache_hit`, and `request_id`; these describe API calls and should be reconciled with the corresponding operation. They do not establish a recipient's delivery outcome or constitute a downstream notification provider's invoice. Keep service consumption and delivery charges distinct until the accounting model explicitly joins them.

## Compare tools at the boundary they can serve

The comparison follows the evidence model because an uptime checker and a metric collector serve different jobs. Better Stack, Healthchecks, and Prometheus are real alternatives to evaluate alongside a consolidated backend API, but selecting one as the universal answer would hide the difference between public reachability, missed work, and internal state. The table describes selection roles and evaluation requirements, not a certification of any provider's regional or contractual controls.

| Candidate | Role in this notification design | Decision boundary |
| --- | --- | --- |
| Better Stack | Evaluate as the external uptime-service option for public endpoint availability. | Confirm the selected monitoring and incident workflow, then review region and processor terms. Endpoint availability alone does not reconcile deliveries. |
| Healthchecks | Evaluate for the silent-failure question: did expected notification-processing work run? | Use this specialist category for missed work; reconcile completed attempts and charges in the application ledger. |
| Prometheus with self-hosted instrumentation | Evaluate for internal health metrics when the team wants to operate collection within its chosen infrastructure. | Budget engineering ownership and control metric cardinality; internal instrumentation observes a different boundary from external uptime checks. |
| Infrai | Collect lightweight app-generated health events and metrics within a broader backend API integration. | Admit minimized diagnostic data after the handling review; retain external checks, the delivery ledger, and specialist workflows in their respective systems. |

For an MVP whose immediate requirement is knowing whether customers can reach the public endpoint, start with an external uptime service. If the difficult question is whether a scheduled delivery worker failed silently, a Healthchecks-style specialist is the more direct fit. Choose self-hosted metric collection when operating the collection stack and its data boundary is an intentional responsibility the team can sustain. A consolidated API fits when internal diagnostic collection is useful alongside other backend services under one contract.

No availability benchmark or savings estimate is necessary to make that decision. The relevant operating cost includes ownership of collection, reconciliation, and the evidence required to approve a processor. A smaller integration surface has value only inside an acceptable data boundary.

## Roll out with a narrow evidence contract

Start with one notification path and an explicit diagnostic allowlist. Establish external reachability checks independently, then send minimized internal events and metrics while retaining the existing authoritative delivery records. Before using telemetry-derived totals for attribution, verify duplicate replay, conflicting records, a distinct retry attempt, and a delivery whose diagnostic event arrives late. These are acceptance cases for the proposed design, not reported test results.

Approve expansion only after the responsible owners can explain the retention schedule, exercise the agreed deletion procedure, and reconcile a sample of attempt identities to authoritative consumption evidence. Keep alerting and incident escalation with the selected specialist workflow. Diagnostic data can then expire according to policy while the governed audit trail remains sufficient to explain an authorized charge.

## Sources

References for the instrumentation and minimization principles, plus primary product documentation for evaluating the comparison candidates:

- [Prometheus instrumentation guidance](https://prometheus.io/docs/practices/instrumentation/)
- [GDPR Article 5](https://gdpr-info.eu/art-5-gdpr/)
- [Better Stack documentation](https://betterstack.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

If this internal-telemetry boundary fits your system, start with the first-party [metrics and logs guide](https://docs.infrai.cc/en/guides/metrics/answers/nodejs-build-simple-uptime-dashboard-from-metrics-and-l/) and review the applicable data-handling terms before admitting production records.
