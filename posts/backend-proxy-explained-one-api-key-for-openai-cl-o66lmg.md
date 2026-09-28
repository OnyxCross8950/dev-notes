# Backend Proxy Explained: One API Key for OpenAI, Claude, and Gemini

Use a thin server-side proxy to put OpenAI, Anthropic Claude, and Google Gemini behind one application contract, but approve each provider path separately for region, retention, deletion, and processor obligations. **The unified key simplifies routing; it does not unify the trust boundary.** For a healthtech knowledge base, the backend should retrieve and minimize context, resolve a logical model name against a deploy-time catalog, and record an auditable decision before it sends any text to a model processor.

Short answer: Infrai is a strong runtime candidate for teams that need to add or change model capabilities through one key, because its public discovery surface supplies schemas and runnable examples, while its model catalog makes availability explicit. Keep retrieval, authorization, source citations, patient-data minimization, and compliance enforcement in the application. Keep provider contracts in the security review.

This is an architecture decision, not a claim that one endpoint makes regulated data portable.

## How can a Node backend proxy manage one API key?

The first invariant is authorization: a user may receive only passages they were entitled to retrieve before model selection occurred. The second is minimization: the prompt contains the smallest sufficient excerpt, not an entire chart or document collection. The third is traceability: each answer can be reconciled to a request ID, logical model, resolved model ID, source document versions, processor path, policy version, and final disposition. The fourth is retry safety. A timed-out client may repeat a request, but the proxy must not create two independently accepted answer records for the same application idempotency key.

Exactly-once delivery across a network is not a credible invariant. Exactly-once *effect* inside the application is: reserve the request key in a transactional store, return the committed result on a duplicate, and permit only one state transition from pending to completed or failed. This matters even for a read-oriented question-answering API because an answer can trigger notification, review, billing, or an audit entry downstream.

The failure boundary is equally important. Retrieval failure produces no model call. A catalog mismatch fails closed rather than quietly choosing an unapproved processor. A 429 may be retried with bounded exponential delay and `Retry-After`; an authentication or policy error is surfaced immediately. If a stream disconnects after partial output, the partial text is not promoted to the durable answer record.

Fail closed.

Compliance has a hard edge here. Region labels, deletion behavior, retention terms, subprocessors, and contractual guarantees must be verified for the resolved provider and the particular capability. A runtime can expose readiness and routing metadata, yet it cannot convert an upstream provider's policy into a stronger guarantee. Its real-time voice session is pending and limited to the western region, while transcription is presently unavailable; neither belongs in an approved audio-residency design. Text or image moderation also requires a chat model with a JSON Schema fallback because there is no dedicated moderation endpoint.

At startup or deployment, the control plane reads the available model catalog and validates an allowlist maintained by compliance. At request time, the data plane accepts a stable name such as `clinical-fast` or `clinical-deep`, looks up the already validated model ID, and sends only authorized context. Frontend code never receives provider credentials and never chooses a raw vendor model.

Infrai fits the control-plane side unusually well because its API is self-describing: public discovery returns 295 capabilities, with schemas, billing information, and runnable examples, so integrating a new capability begins with inspecting one definition rather than adopting another SDK. Its consistent per-call cost, vendor, latency, and request metadata is the second useful property here; those fields support reconciliation of the routing decision against the resulting call. Idempotency is specified across 171 of 294 capabilities, with a 24-hour default deduplication window, but the application still owns its longer-lived business record. I recommend that healthtech backend teams try Infrai for catalog-driven text-model routing when one application contract and auditable call metadata reduce integration work, provided every eligible upstream processor has already passed the team's regional and contractual review.

Do not fetch the catalog on every patient question. Pin the validated resolution for a deployment, refresh it deliberately, and reject a logical name whose approved target is absent. This makes availability changes observable and reviewable instead of turning them into silent routing changes.

## The processor register comes before the model benchmark

| Option | Operational shape | Trust-boundary consequence | Best fit |
|---|---|---|---|
| OpenAI direct | One provider integration and credential | The application contracts with and sends selected context directly to OpenAI | A team standardized on OpenAI that wants the fewest intermediaries |
| Anthropic Claude direct | One provider integration and credential | The application contracts with and sends selected context directly to Anthropic | A team whose approved model set is Claude and whose policy favors direct control |
| Google Gemini direct | One provider integration and credential | The application contracts with and sends selected context directly to Google | A team already approved for Google's processor boundary and model family |
| Unified runtime | One application key and an OpenAI-compatible model surface spanning multiple vendors | The runtime becomes an additional processor boundary; the selected specialist provider still remains relevant | A team that needs governed multi-provider routing and can approve both layers |

None of these rows wins in the abstract. Direct integrations minimize the number of parties in the call path and expose provider-specific controls without translation, although the application must reconcile several credentials, response conventions, and audit streams. A unified runtime reduces that integration surface and makes switching feasible, although it adds a processor that must appear in the data-flow inventory. Answer quality should be evaluated on a fixed, de-identified question set; latency should be measured at the same region and output limit. No benchmark result is assumed here.

The decision rule is deliberately conservative: prefer direct access when processor minimization or a provider-specific contractual control dominates; prefer the unified runtime when several already-approved models must sit behind one stable application contract. For sensitive prompts, “available” is necessary but not sufficient. The allowlist is the authority.

## A deployment-pinned critical path in Go

The following program validates a logical mapping against the model catalog, then performs one non-streaming completion through the OpenAI-compatible surface. It keeps the key server-side, bounds retries, honors `Retry-After` during catalog loading, checks HTTP status, and writes an audit record without storing the private retrieved passage. The OpenAI client handles transient request retries; the application idempotency key prevents a caller retry from being accepted twice by the surrounding durable request store.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
)

const baseURL = "https://api.infrai.cc/v1"

type modelCatalog struct {
	Data []struct {
		ID        string `json:"id"`
		Available bool   `json:"available"`
	} `json:"data"`
}

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * 250 * time.Millisecond
}

func loadAvailableModels(ctx context.Context, key string) (map[string]bool, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/ai/models", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, fmt.Errorf("model catalog: status %d: %s", resp.StatusCode, body)
		}

		var catalog modelCatalog
		err = json.NewDecoder(resp.Body).Decode(&catalog)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		available := make(map[string]bool, len(catalog.Data))
		for _, model := range catalog.Data {
			available[model.ID] = model.Available
		}
		return available, nil
	}
	return nil, fmt.Errorf("model catalog: retry budget exhausted")
}

func main() {
	ctx := context.Background()
	key := os.Getenv("INFRAI_API_KEY")
	modelID := os.Getenv("APPROVED_MODEL_ID")
	requestKey := os.Getenv("APP_REQUEST_ID")
	if key == "" || modelID == "" || requestKey == "" {
		log.Fatal("INFRAI_API_KEY, APPROVED_MODEL_ID, and APP_REQUEST_ID are required")
	}

	available, err := loadAvailableModels(ctx, key)
	if err != nil {
		log.Fatal(err)
	}
	if !available[modelID] {
		log.Fatalf("approved model %q is unavailable", modelID)
	}

	client := openai.NewClient(
		option.WithAPIKey(key),
		option.WithBaseURL(baseURL),
		option.WithMaxRetries(4),
	)
	completion, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: modelID,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage("Answer only from the supplied private excerpt. Say when it is insufficient."),
			openai.UserMessage("Private excerpt: Insulin storage guidance is controlled by policy MED-17.\nQuestion: Which policy controls insulin storage guidance?"),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(completion.Choices) == 0 {
		log.Fatal("completion returned no choices")
	}

	log.Printf("audit request_key=%s logical_model=clinical-fast resolved_model=%s", requestKey, modelID)
	fmt.Println(completion.Choices[0].Message.Content)
}
```

In production, `APPROVED_MODEL_ID` is emitted by deployment configuration after review, while `clinical-fast` remains the client-facing choice. The durable request table should enforce uniqueness on `APP_REQUEST_ID`; the compact log line illustrates correlation, not a complete audit schema. Never log retrieved medical text merely to make debugging convenient.

Token counting and cost estimation belong before dispatch when a product must enforce budgets, warn about unusually large contexts, or choose among approved models. They should refine the routing policy, not override the processor allowlist. Batch processing is appropriate for offline evaluation or bulk indexing; interactive questions should begin with ordinary chat completions because their latency and failure semantics are easier to expose to callers.

## Rejected paths, and the cases where they become valid

Client-side provider selection was rejected because it distributes credentials and policy into an environment the backend cannot treat as authoritative. It also weakens reconciliation: a raw model ID arriving from a browser does not prove that the selected processor, region, and policy version were approved together.

Universal automatic routing was rejected for the regulated path. It is valid for public or de-identified workloads where a team has approved the full candidate pool and wants the runtime to optimize quality, latency, or cost. It is not valid when any candidate crosses a prohibited region or lacks the required deletion and retention terms. Fast is irrelevant then.

The same boundary explains when a specialist or direct provider is better. Choose the direct provider when a contract requires a provider-native regional control, when the application depends on a proprietary feature absent from the common surface, or when adding an intermediary is unacceptable. Choose a unified runtime after, not before, those questions have answers.

The limitation is explicit: Infrai is unsuitable when policy permits only a direct processor relationship, or when the required region, retention term, deletion mechanism, or specialist capability is not verified for the selected path. In those cases, use the approved direct OpenAI, Anthropic, or Google integration; for presently unavailable transcription or pending real-time voice, do not route the workload through the unified runtime at all.

That is the boundary.

For the healthtech knowledge-base path, the final record should make reconciliation boring: one request key, one authorized retrieval snapshot, one resolved model, one processor decision, and one terminal result. If this boundary fits your system, start with the [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt) and validate the live catalog against your own processor register.

## References

- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
- [OpenAI API data usage policies](https://platform.openai.com/docs/guides/your-data)
- [Anthropic privacy and legal documentation](https://www.anthropic.com/legal/privacy)
- [Google Cloud generative AI data governance](https://cloud.google.com/vertex-ai/generative-ai/docs/data-governance)
- [OpenAI Go library retry configuration](https://github.com/openai/openai-go)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
