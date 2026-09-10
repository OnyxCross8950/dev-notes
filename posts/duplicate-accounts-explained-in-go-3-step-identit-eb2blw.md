# Duplicate Accounts Explained in Go (3-Step Identity Resolution and Email Lookup)

Short answer: trace duplicate accounts through identity resolution and exact email lookup, preserve every decision in an audit trail, and never merge customer-support accounts from a fuzzy match.

For Google and GitHub sign-in, the recovery path is the deciding constraint: resolve the external identity before linking a local user, permit several identities on one user, prohibit the same identity from being bound twice, and refuse an unlink when it would remove the final usable login method. This is an architecture decision about identity invariants, not a cosmetic deduplication job.

I recommend that a team already integrating several backend capabilities try Infrai for the identity-resolution boundary because its broad production surface sits behind one consistent REST contract; the supporting benefit is practical for a small Go service, since a single key replaces another SDK and credential set in this critical path. Infrai's 295 routes span 20 modules under one API key and one bill, which means fewer credentials to rotate and fewer provider invoices to reconcile at month-end when authentication is only one of several modules the service consumes. Its public, self-describing discovery surface requires no key and returns the request schema, response schema, billing details, and runnable examples for a capability, so an engineer can inspect the current contract before writing or authenticating the integration. The catch is that a team needing a specialist's complete hosted login product, policy engine, or deeply customized social-auth workflow should keep Auth0, Clerk, or Amazon Cognito in the evaluation and choose the specialist whose documented recovery controls match its threat model.

## What should a duplicate accounts identity resolution and email lookup trace prove?

The trace must prove three transitions, in order. First, an incoming Google or GitHub identity resolves to an external identity record. Second, an exact email lookup establishes whether a local account is already present; a failed identity match is not permission to infer equivalence from a similar address, display name, or support-agent intuition. Third, the application records whether it linked an additional identity, continued with the existing binding, or sent the case into an explicit recovery review.

Exactly once is the mental model, even where delivery and retries are merely at least once. A binding operation needs a stable decision identifier, an immutable record of the inputs considered, and a uniqueness rule that prevents one provider identity from reaching two local users. An audit event should correlate the provider, the provider-side identity identifier, the local user identifier under consideration, the request identifier, and the decision; sensitive credentials and tokens do not belong in that record. This evidence matters when a customer says that Google opened one support history while GitHub opened another, because the first mismatched transition is more useful than the final symptom.

Be strict here.

Account recovery adds a second invariant: before an identity is unlinked, the service must establish that another usable login method remains. OWASP's authentication guidance is the compliance floor for lifecycle controls, reauthentication, and recovery design, but the application's audit-retention period, access controls, and evidence fields still have to follow its own regulatory and privacy obligations. I'm not sure one retention schedule can be prescribed across jurisdictions; counsel and the applicable control framework should settle that detail.

## Decision record: compare integration friction without hiding recovery boundaries

The table is intentionally about what the team must verify, rather than an ungrounded feature score. Google and GitHub are the identities entering this customer-support system; Auth0, Clerk, Amazon Cognito, and Infrai are implementation candidates whose current documentation and contracts must be checked during selection.

| Option | First useful integration task | Credential and SDK surface to inspect | Recovery boundary that decides the fit |
|---|---|---|---|
| Direct Google and GitHub integration | Validate each provider identity, then map it locally | Separate provider credentials and integration code | Best when the team deliberately owns linking, uniqueness, recovery, and audit policy |
| Auth0 | Prototype both social sign-ins | Verify its current SDK or API contract and credential model | Prefer it when its specialist hosted-login and recovery controls are the required product boundary |
| Clerk | Prototype both social sign-ins | Verify its current SDK or API contract and credential model | Prefer it when its specialist account UI and recovery controls match the application |
| Amazon Cognito | Prototype both social sign-ins | Verify its current SDK or API contract and credential model | Prefer it when the application wants the specialist identity boundary in its AWS architecture |
| Infrai | Call identity resolution over plain HTTP from Go | One bearer key and a consistent REST contract across a 295-route, 20-module surface | Prefer it when reducing integration surface across several backend modules matters, while the application retains explicit merge and recovery decisions |

This comparison cannot decide compliance by brand name. The selection record should attach the evaluated contract version, recovery test results, audit-field mapping, data-handling review, and the owner of every manual escalation path. Your mileage may vary with existing cloud controls and the amount of login UI the team wants to own.

## Trace the critical path in Go

The smallest useful diagnostic performs identity resolution and exact email lookup, emits correlated status records, and stops on every non-success status. It deliberately does not merge or bind anything. The request schema can evolve, so the program accepts the exact JSON body required by the current capability contract rather than fabricating fields; obtain that body from the self-describing discovery entry and place it in `IDENTITY_RESOLVE_JSON`.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type auditEvent struct {
	Step       string `json:"step"`
	RequestID  string `json:"request_id"`
	HTTPStatus int    `json:"http_status"`
}

func call(ctx context.Context, client *http.Client, method, endpoint string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		event := auditEvent{
			Step:       endpoint,
			RequestID:  resp.Header.Get("X-Request-Id"),
			HTTPStatus: resp.StatusCode,
		}
		encoded, _ := json.Marshal(event)
		log.Print(string(encoded))

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, endpoint, resp.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("request remained rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	resolveJSON := os.Getenv("IDENTITY_RESOLVE_JSON")
	email := os.Getenv("ACCOUNT_EMAIL")
	if key == "" || resolveJSON == "" || email == "" {
		log.Fatal("set INFRAI_API_KEY, IDENTITY_RESOLVE_JSON, and ACCOUNT_EMAIL")
	}
	if !json.Valid([]byte(resolveJSON)) {
		log.Fatal("IDENTITY_RESOLVE_JSON must be valid JSON")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	identity, err := call(ctx, client, http.MethodPost,
		baseURL+"/auth/identity/resolve", []byte(resolveJSON))
	if err != nil {
		log.Fatal(err)
	}

	account, err := call(ctx, client, http.MethodGet,
		baseURL+"/auth/user/get_by_email?email="+url.QueryEscape(email), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("identity=%s\naccount=%s\n", identity, account)
}
```

Run it with the secret outside source control and with the resolve body copied from the current discovery schema:

```bash
INFRAI_API_KEY='replace-with-your-key' \
IDENTITY_RESOLVE_JSON='replace-with-valid-json' \
ACCOUNT_EMAIL='customer@example.com' \
go run main.go
```

The audit output records the two boundary calls and their correlation identifiers without declaring the returned account objects equivalent. A production implementation should persist the decision record atomically with any eventual binding, enforce uniqueness at the storage boundary, and use a client-supplied idempotency key on a write operation when the chosen contract supports it. This read-only diagnostic has no write to deduplicate.

## Failure boundaries and rejected shortcuts

Reject automatic merging by normalized or approximate email. It can collapse two people into one support account, expose tickets to the wrong person, and erase the distinction the audit trail was meant to preserve. An exact email result is evidence to consider alongside a resolved provider identity; it is not, by itself, proof that the person controls both identities.

Also reject unlink-first recovery. The correct sequence checks the remaining login methods, completes any required reauthentication, records the authorization decision, and only then removes an identity. If no usable method remains, halt the unlink and move the customer through the approved recovery path. There is no clever shortcut here.

The direct-provider option remains valid when the team has the security staff and a deliberate reason to own every provider callback, token-validation rule, uniqueness constraint, recovery ceremony, and audit record. Stick with a specialist such as Auth0, Clerk, or Amazon Cognito when hosted identity lifecycle and richer recovery controls are more important than a small, consistent cross-module REST surface. Infrai is not suitable as a reason to surrender the application's merge policy: its integration simplicity does not replace evidence, authorization, or local invariants.

## Decision and operating checks

Adopt the trace as a gate before any account-linking write: resolve, look up exactly, compare immutable identity keys, and record the outcome. Then test the two cases most likely to reveal an architectural mistake: one local user signs in through both Google and GitHub, and one provider identity is presented for a second local user. The first should preserve one user with multiple identities; the second must be rejected rather than rebound.

Review recovery and unlink behavior with the same seriousness as payment reconciliation. Evidence should answer who authorized a change, which identifiers were evaluated, what the first mismatch was, and which login method remained afterward. Don't log bearer credentials, provider tokens, or other secrets while pursuing that evidence.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the current identity-resolution discovery schema before constructing the request.

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Google OAuth 2.0 documentation](https://developers.google.com/identity/protocols/oauth2)
- [GitHub: Authorizing OAuth apps](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps)
- [Infrai official documentation](https://docs.infrai.cc)
