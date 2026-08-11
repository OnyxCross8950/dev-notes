# Per-Tenant Cost Visibility for a Startup App Using One API Key

Short answer: for a startup app using one API key across OpenAI and other model providers, compare token cost only after the router proves that each choice fits the tenant's quality, compliance, and budget rules. For a property-management code-review service, the cheapest request is the one whose result can be traced, reconciled, and acted on without a second manual review.

That decision rule is deliberately less exciting than a price table. It is also more useful. A property manager may submit a small patch for one building and a large migration for another; averaging their token usage hides the tenant that is actually subsidizing the system. Per-tenant cost visibility must therefore be an accounting invariant, not a dashboard decoration.

Costs drift.

Suppose a tenant's small patch passes preflight at 18,000 estimated tokens, then a retry adds a generated explanation after schema validation rejects the first response. If the ledger records only the final answer, the tenant appears to have used one ordinary review; if it records the preflight, both attempts, the validation failure, and the published result, finance can explain the variance and engineering can decide whether the policy should lower the output ceiling. That chain is longer than a token-rate comparison, but it is the part a property-management customer can actually audit when a monthly bill is disputed.

## What should a one-key router measure before a code review?

Start with the unit of work. Give every review an operation ID, tenant ID, repository commit, policy version, task class, input-token estimate, output budget, selected model, returned usage, and final charge. The estimate is a forecast; the returned usage is evidence. Keep both. A later tokenizer change must not rewrite the explanation for an old charge.

The router should reject a request before dispatch when its context exceeds the tenant's configured budget, when the repository contains a file type outside the review policy, or when the response contract cannot be checked. A cheaper model that emits an incomplete finding list can create a second review, and the second call belongs in the cost calculation. Price is downstream of correctness.

A useful preflight record looks like this:

| Field | Why it matters for property management |
| --- | --- |
| tenant_id | Separates a portfolio's spend from another portfolio's spend |
| operation_id | Joins retries, responses, and one business result |
| commit_sha | Makes a finding reproducible against the reviewed code |
| policy_version | Explains why a model was eligible on that date |
| estimated_tokens | Sets a pre-dispatch budget |
| actual_tokens | Supports invoice reconciliation |
| finding_count and severity | Tests usefulness, not just low usage |

This is where an exactly-once mindset matters. HTTP retries are not exactly once. They can produce multiple attempts after a timeout, even when the service eventually returns one answer. Store every attempt under the same operation ID, and let an idempotent state transition publish one review result. The audit trail should show the 429 response, the timeout, or the successful second attempt without turning those attempts into duplicate findings.

Three words: record the decision.

## How can a startup app compare token cost across models without fooling itself?

Compare complete work, not an isolated input-token rate. For each candidate, calculate a forecast from the same prompt, repository slice, requested output ceiling, and retry policy. Then compare the forecast with actual usage after the response. A model with a lower nominal input rate may consume more output tokens or require more repair calls; a model with a higher rate may finish the review in one valid response. The relevant row is the cost of an accepted finding set per tenant.

Use a fixed evaluation corpus drawn from the actual job: small controller edits, dependency upgrades, authorization changes, and a deliberately awkward migration diff. Label expected findings and false positives. Keep the corpus versioned. The purpose is not to declare a permanent winner; it is to establish a minimum acceptance test that a routing policy cannot quietly trade away for a lower bill.

A review response should have a machine-checkable shape. A JSON Schema-style contract can require a severity, file path, line reference, explanation, and remediation for each finding, with an explicit empty array for a clean review. Structured output guidance describes why constraining generation to a schema helps downstream code consume a response, but schema validation still cannot prove that a finding is correct.

I treat a 429 as an attempt record, not as permission to charge a second business operation. Back off within a bounded retry budget, preserve the operation ID, and make the final accounting include every provider-reported usage value. I'm not sure a startup needs a learned router at this stage; your mileage will vary with tenant volume, task diversity, and how expensive a false negative is.

The comparison should be visible at tenant granularity:

```go
package cost

import "time"

type Preflight struct {
	TenantID       string
	OperationID    string
	CommitSHA      string
	PolicyVersion  string
	EstimatedInput int
	OutputLimit    int
}

type Attempt struct {
	OperationID  string
	Model        string
	InputTokens  int
	OutputTokens int
	Status       int
	StartedAt    time.Time
}

type Finding struct {
	Severity string
	Path     string
	Line     int
	Summary  string
}

// The caller persists the preflight and every attempt before publishing findings.
func Acceptable(p Preflight, a Attempt, findings []Finding, tenantBudget int) bool {
	if a.OperationID != p.OperationID {
		return false
	}
	if a.InputTokens+a.OutputTokens > tenantBudget {
		return false
	}
	for _, f := range findings {
		if f.Severity == "" || f.Path == "" || f.Line < 1 || f.Summary == "" {
			return false
		}
	}
	return a.Status >= 200 && a.Status < 300
}
```

The function does not calculate a currency amount because currency schedules and tokenization rules belong to the selected integration's verified configuration. It checks the invariant that makes a later calculation trustworthy: the response is tied to the right tenant and operation, stays inside the configured token budget, and satisfies the review contract. A production implementation should also persist the policy decision atomically with the acceptance state; otherwise a dashboard can show a model choice that the ledger cannot reproduce.

## Which architecture makes per-tenant cost visible at failure time?

Separate the control plane from the inference path. The control plane owns tenant budgets, allowed task classes, model eligibility, schema versions, and policy approvals. The inference path receives an immutable decision, sends one normalized request, validates the response, and emits an attempt event. This separation makes a route change reviewable in a pull request instead of burying it in a request handler.

A minimal event sequence is: `review.requested`, `review.preflighted`, `review.dispatched`, `review.attempted`, `review.validated`, and `review.published`. Each event carries the operation ID and tenant ID. The final published event is idempotent; an event consumer can see the same message twice and still create one review result. Reconciliation then sums accepted usage by tenant and task class while retaining rejected and retried attempts for explanation.

Streaming changes the observation problem. Server-Sent Events are a one-way stream from server to browser, so a review UI can show incremental findings while the backend remains the source of truth for validation and billing. Do not mark a review complete merely because the browser received the last visible chunk. Persist the validated result first, then notify the client that the operation is complete. The browser may disconnect; the accounting record must not.

For auditability, retain the prompt fingerprint and repository commit rather than assuming that a mutable branch name identifies the input. Apply the smallest retention period compatible with contractual and compliance obligations, and redact secrets before persistence. A cost ledger is not permission to retain source code forever. PCI DSS scope, privacy law, contractual residency requirements, and internal deletion policies can constrain what is stored; the router should surface those constraints as eligibility rules, not as an afterthought in finance reporting.

## What trade-offs belong in a fair router decision?

There are three broad operating shapes, and each moves risk to a different place. Direct provider integrations give native controls but require multiple credentials, adapters, usage formats, and failure semantics. A hosted gateway can normalize access and policy, but adds a trust boundary and a dependency whose usage export and retention terms must be reviewed. A self-hosted proxy keeps more control in the team's environment, while the team owns deployment, upgrades, availability, and secret rotation. None is automatically cheapest once on-call time and reconciliation work are counted.

| Operating shape | Strength | Constraint to document |
| --- | --- | --- |
| Direct integrations | Native controls and provider-specific features | More adapters, keys, invoices, and retry semantics |
| Hosted gateway | One integration surface and centralized policy | Extra trust boundary and dependency for usage evidence |
| Self-hosted proxy | Control over placement and release process | Your team owns operations, upgrades, and availability |

The catch is that one API key does not create one compliance boundary. It can simplify secret rotation and request plumbing, but tenant isolation, data retention, residency, and invoice reconciliation remain application responsibilities. A hosted layer is not suitable when its evidence export or data-handling terms cannot satisfy the tenant contract. Stick with direct integrations when provider-native controls are mandatory. Use a proxy when the operational burden of many adapters is the binding constraint and the team can operate it honestly.

Do not compare options with a single `cost_per_token` column. Record at least currency, token category, context size, output ceiling, retry count, validation failures, latency percentile, and accepted-findings rate. Then report cost per accepted review by tenant. That metric makes the trade-off legible without pretending that a model's published token figure predicts every workload.

## A controlled rollout protects the review ledger

Run shadow preflight first. Estimate and classify the existing workload without sending a second paid request, then compare the predicted token bands with the usage already reported by the current integration. Investigate outliers by tenant and task class. If the estimator consistently misses, fix the measurement boundary before changing the routing policy.

Next, enable one low-risk review class behind a feature flag. Cap spend per tenant and per day, sample validated findings for human review, and make rollback a policy-version change rather than a code emergency. A deployment is ready only when engineering can answer which policy selected the model, finance can rebuild a day's total from attempts, and support can locate the reviewed commit from an operation ID.

The durable rule is straightforward: choose the lowest-cost eligible route for an accepted review, then preserve enough evidence to challenge that choice later. One credential may reduce integration friction. It cannot replace idempotency, an audit trail, schema validation, or explicit compliance limits.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://platform.openai.com/docs/guides/structured-outputs
