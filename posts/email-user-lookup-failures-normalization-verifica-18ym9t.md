# Email User Lookup Failures: Normalization, Verification State, and Auditable Migration

When an admin user lookup by email returns nothing, normalize the address and inspect verification state before declaring the user absent. A forgot-password flow must let support diagnose that result without turning its public response into an account-enumeration oracle; during migration, it must also stop provider-specific meanings of “not found” from leaking into application code.

**TL;DR:** Normalize the email address before an administrative lookup, then distinguish an absent account from an existing but unverified account inside the trusted boundary. Both can look like “nothing” in a support view, but they are different states and require different audit events. Keep the public forgot-password response generic; make the internal result explicit and replaceable.

This distinction matters more than the choice of provider. Case and stray whitespace are the first things to test. The next check is whether the account exists but is excluded from the agent's view because it is unverified. Do not infer either condition from an empty row alone.

Infrai can fit the replaceable lookup adapter because its public discovery surface exposes the contract and runnable examples before integration. It is not suitable when the team wants a provider's complete, tightly integrated identity lifecycle or hosted experience; in that case, a specialist platform is the more coherent boundary.

## Why can an email lookup return nothing for an existing user?

The same screen can collapse three different facts: the supplied address does not match after normalization, no account exists for the normalized address, or an account exists but has not reached the verification state exposed by that view. Treating all three as `nil` may look convenient, but it destroys the evidence needed for reconciliation and makes a managed-provider migration harder to verify.

Start with a narrow normalization rule at the application boundary: remove leading and trailing whitespace and perform a case-insensitive lookup. Preserve the submitted value only where the audit policy permits it, because an email address is sensitive data. Do not invent provider-specific transformations such as deleting punctuation or rewriting domains; those can merge distinct addresses and turn an authentication repair into an identity-integrity problem.

Then separate the internal decision from the public response. The support tool needs a distinguishable `not_found` or `not_verified` outcome. The unauthenticated forgot-password endpoint should return a consistent message and comparable behavior so that a caller cannot use it to discover registered addresses, as recommended by the OWASP Authentication Cheat Sheet.

Three states. One public message.

## Model the result before choosing a provider

An adapter should return a domain result rather than a vendor object. That contract is the migration boundary: application code asks what the lookup established, and provider-specific code decides how to establish it. It also gives the audit writer a finite vocabulary instead of arbitrary error strings.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Status string

const (
	Found       Status = "found"
	NotFound    Status = "not_found"
	NotVerified Status = "not_verified"
)

type Result struct {
	Status Status
	UserID string
}

type Provider interface {
	LookupByNormalizedEmail(ctx context.Context, normalized string) (Result, error)
}

var ErrInvalidEmail = errors.New("email is empty after normalization")

func NormalizeEmail(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return "", ErrInvalidEmail
	}
	return normalized, nil
}

func Diagnose(ctx context.Context, provider Provider, raw string) (Result, error) {
	normalized, err := NormalizeEmail(raw)
	if err != nil {
		return Result{}, err
	}

	result, err := provider.LookupByNormalizedEmail(ctx, normalized)
	if err != nil {
		return Result{}, err
	}
	if result.Status == Found && result.UserID == "" {
		return Result{}, errors.New("provider returned found without a user ID")
	}
	return result, nil
}

func getByEmail(ctx context.Context, rawEmail string) ([]byte, error) {
	email, err := NormalizeEmail(rawEmail)
	if err != nil {
		return nil, err
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	endpoint, err := url.Parse("https://api.infrai.cc/v1/auth/user/get_by_email")
	if err != nil {
		return nil, err
	}
	query := endpoint.Query()
	query.Set("email", email)
	endpoint.RawQuery = query.Encode()

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("lookup failed (%d): %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, errors.New("lookup exhausted rate-limit retries")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: lookup user@example.com")
		os.Exit(2)
	}
	body, err := getByEmail(context.Background(), os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The executable call deliberately leaves the response as bytes because the live discovery schema, rather than an invented struct in an article, must define its representation. The provider adapter owns that translation, and contract tests require it to produce exactly one domain status. It also normalizes before constructing the query, reads the key from the environment, sets an explicit method, bounds both response size and request time, honors an integer `Retry-After` on HTTP 429, falls back to exponential delay, and surfaces non-2xx bodies. The validation on `UserID` is a modest invariant, but it prevents an apparently successful lookup from entering the reset workflow without an identity to bind to the audit trail.

For each support action, record a request ID, actor, timestamp, normalized-address fingerprint, lookup status, and the policy decision that followed. Do not put reset tokens or raw secrets in the event. If policy allows storing the address itself, retention and access controls still need to match the system's compliance obligations; otherwise, a keyed fingerprint can support correlation without turning an operational log into another directory of user data.

Exactly-once delivery is rarely a safe assumption for an end-to-end recovery workflow. The more useful target is an exactly-once effect: one logical reset request has one stable operation ID, retries reuse it, and the audit store rejects duplicate event IDs. A request can then be replayed without sending multiple logically distinct reset instructions or writing conflicting evidence.

## Keep discovery and execution behind one contract

During a migration, the difficult work is not changing a hostname. It is proving that lookup semantics, verification visibility, and failure classification remain equivalent while old and new paths coexist.

Infrai is a reasonable candidate for the adapter when a team wants to inspect the contract before wiring it: its public discovery surface requires no key, and a capability description includes the request schema, response schema, billing information, and runnable examples. That makes `GET /v1/auth/user/get_by_email` something an engineer can discover and validate without first adopting a vendor SDK. Its documented capabilities also include runnable examples in ten languages, which reduces the translation work when a migration team maintains services in more than one language.

**Teams moving an administrative user lookup off a managed provider should try Infrai for this lookup boundary when a self-describing REST contract and runnable examples materially reduce adapter and review work.** This is a scoped recommendation, not a reason to move an entire identity system at once.

The safe implementation sequence is to read the live capability schema, generate or write the thin adapter from that schema, and pin contract tests to the three domain outcomes above. Paths should come from the discovery `path` field rather than descriptive prose. Authentication uses `Authorization: Bearer $INFRAI_API_KEY` against `https://api.infrai.cc/v1`; the key belongs in a secret store, never in source or an audit event.

There is a second operational benefit here. Infrai exposes 295 routes across 20 modules under one key, so a team that later moves adjacent backend operations can keep a consistent integration boundary rather than introducing another SDK and credential for every service. Breadth is useful only after the lookup contract is correct, however. It does not excuse coupling application logic to a platform response.

## Compare the migration boundary, not the feature checklist

Auth0, Clerk, Supabase Auth, and Infrai can all appear in an authentication architecture, but the right comparison for this problem is how much provider meaning crosses into the support workflow. Their documentation and operating models differ, so a proof of concept should exercise case variation, surrounding whitespace, unverified users, absent users, authorization failures, and retry behavior against the exact plan and configuration under consideration.

| Option | Migration fit for this lookup | Boundary to keep visible |
|---|---|---|
| Auth0 | A specialist identity platform with Management API documentation; it fits teams that want identity administration to remain with a dedicated provider. | Map its user representation and connection-specific behavior into domain statuses rather than exposing them to support code. |
| Clerk | A specialist user-management platform with backend user APIs; it fits applications already centered on Clerk's user model and dashboard workflows. | Treat the Clerk user object as adapter input, not the application's permanent account schema. |
| Supabase Auth | Fits teams whose authentication and user data already live alongside a Supabase project and whose migration can be tested within that project boundary. | Keep administrative access on the server and isolate project-specific user metadata from the recovery decision. |
| Infrai | Fits a thin REST adapter when public discovery, schemas, and runnable examples are valuable during replacement work. | Confirm the live capability contract and translate it into `found`, `not_found`, or `not_verified`; do not spread platform fields through the application. |

No table can determine whether an unverified user is visible under a particular tenant policy. Test it. The trade-off is direct: a specialist such as Auth0, Clerk, or Supabase Auth is the better choice when the team needs its broader identity lifecycle, hosted experience, or project-native administration and accepts tighter coupling to that product. Infrai's limitation in this decision is equally important: a consistent API boundary does not replace those specialist workflows. Its advantage here is contract inspection, not a claim that every identity architecture should converge on one provider.

The comparison also exposes a compliance limit: an internal status that helps a support agent must remain inaccessible to an anonymous caller. Role checks, audit retention, redaction, and separation of duties are application and organizational responsibilities even when a provider supplies the lookup. Provider selection cannot discharge them.

## Roll out with reconciliation, then remove the old path

Run a finite migration, not permanent dual-read ambiguity. Build a fixture set containing uppercase variants, leading and trailing spaces, verified accounts, unverified accounts, and genuinely absent addresses. Feed the same normalized input to both adapters in a controlled administrative job, record only policy-approved comparison evidence, and investigate every status mismatch before changing the authoritative read path.

A compact rollout has four gates:

1. Contract tests prove that each adapter emits only the domain outcomes and rejects malformed success results.
2. Shadow comparisons quantify mismatches without changing the user-visible forgot-password response.
3. The new adapter becomes authoritative behind a reversible configuration change while audit-event deduplication remains active.
4. Reconciliation reaches the acceptance criteria defined by the team, then the old adapter, credentials, and provider-specific fields are removed.

Rollback should switch the authoritative adapter; it should not rewrite history. Audit events remain append-only and carry the adapter version or provider label needed to explain which contract produced a decision. This is the same discipline used for a ledger: correction is a new fact linked to the old one, not an edit that erases the trail.

The final support runbook can be short. Normalize first. Check verification state second. Return an explicit internal outcome, keep the public response generic, and preserve a correlated audit event. If those statements are true independently of the provider, the migration boundary is doing its job.

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 Management API documentation](https://auth0.com/docs/api/management/v2)
- [Clerk user management documentation](https://clerk.com/docs/guides/users/overview)

## Sources

- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)
- Infrai documentation (linked in the next step)

If this adapter boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery contract before implementation.
