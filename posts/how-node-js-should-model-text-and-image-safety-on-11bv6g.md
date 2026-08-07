# How Node.js Should Model Text and Image Safety on an OpenAI-Compatible API

Short answer: use a chat-completions request with a strict JSON Schema, then make your application—not the model—the authority for `allow`, `review`, and `block`. There is no dedicated moderation endpoint in this capability set, so the same contract must cover text and, when the selected model supports it, image input.

That constraint is useful. A moderation result becomes an ordinary, auditable decision rather than a special side channel that each provider models differently. In payment and ledger systems I care about the same property: one input, one durable decision, and a record that explains which policy and model produced it.

## How can Node.js content moderation use chat completions and JSON for image safety?

Start with a small vocabulary. The classifier returns a `decision` whose value is `allow`, `review`, or `block`; a `categories` array drawn from `hate`, `sexual`, `violence`, `self-harm`, `harassment`, and `spam`; and a short `reason`. Set `additionalProperties` to false and make every field required. A free-form explanation can be useful to an operator, but it must not be the thing that drives an account or ledger mutation.

The service should create a decision ID before it calls the model. Persist the content digest, policy version, selected model, region, and request timestamp with that ID. Store the structured response, or a privacy-preserving representation of it, beside the normalized action. If the response cannot be parsed or does not satisfy the schema, write `review`; do not infer safety from a transport success.

Images have one extra gate. Check the model catalog in the intended US or EU region and confirm image input support before accepting an image job. If the model cannot inspect images, send the item to review or choose a supported model. A caption generated elsewhere is not evidence that the original pixels were classified.

Fail closed.

Compliance boundaries remain local. PCI DSS scope, retention periods, cross-border transfer rules, and appeal handling are policy decisions; a model label does not settle them. Your mileage may vary on category thresholds across languages, so keep those thresholds versioned and test them with a representative evaluation set.

## How should Node.js choose a model and call chat completions?

Discovery is a prerequisite, not an optimization. Query the provider's model catalog and, when you need details for one candidate, fetch that model's record. Select a model marked available in the deployment region and verify its modalities for image work. Cache the discovery result briefly, because availability can change, but retain the model ID used for every decision.

The moderation request goes to `/v1/chat/completions` with the schema in `response_format`. Use an environment variable for the bearer key and an explicit HTTP method. A retry after HTTP 429 must honor `Retry-After` when present and use exponential backoff; every write-like moderation attempt should carry the same client-generated idempotency key so a retry cannot create a second logical decision.

Here is the transport core in Go. It is intentionally compact; a Node.js HTTP handler can call the same contract through its OpenAI-compatible client, while the persistence and policy code stays in your service.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type result struct {
	Decision   string   `json:"decision"`
	Categories []string `json:"categories"`
	Reason     string   `json:"reason"`
}

func call(ctx context.Context, client *http.Client, base, key, decisionID string, payload []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/chat/completions", bytes.NewReader(payload))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", decisionID)
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		body := make([]byte, 1<<20)
		n, readErr := resp.Body.Read(body)
		resp.Body.Close()
		if readErr != nil && n == 0 { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil { delay = time.Duration(seconds) * time.Second }
			select { case <-time.After(delay): continue; case <-ctx.Done(): return nil, ctx.Err() }
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("API status %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:n]))) }
		return body[:n], nil
	}
	return nil, fmt.Errorf("review required: retry budget exhausted")
}

func main() {
	key, base, text, decisionID := os.Getenv("INFRAI_API_KEY"), strings.TrimRight(os.Getenv("OPENAI_BASE_URL"), "/"), os.Getenv("MODERATION_TEXT"), os.Getenv("DECISION_ID")
	model := os.Getenv("MODEL_ID")
	if key == "" || base == "" || text == "" || decisionID == "" || model == "" { panic("required environment variable is missing") }
	schema := map[string]any{"type": "object", "additionalProperties": false, "required": []string{"decision", "categories", "reason"}, "properties": map[string]any{
		"decision": map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
		"categories": map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": []string{"hate", "sexual", "violence", "self-harm", "harassment", "spam"}}},
		"reason": map[string]any{"type": "string"},
	}}
	payload := map[string]any{"model": model, "messages": []any{map[string]any{"role": "system", "content": "Classify the supplied content. Return only the schema result."}, map[string]any{"role": "user", "content": text}}, "response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "moderation_decision", "strict": true, "schema": schema}}}
	body, err := json.Marshal(payload)
	if err != nil { panic(err) }
	data, err := call(context.Background(), &http.Client{Timeout: 40 * time.Second}, base, key, decisionID, body)
	if err != nil { panic(err) }
	var envelope struct{ Choices []struct{ Message struct{ Content string `json:"content"` } `json:"message"` } `json:"choices"` }
	if json.Unmarshal(data, &envelope) != nil || len(envelope.Choices) == 0 { panic("review required: invalid completion envelope") }
	var out result
	if json.Unmarshal([]byte(envelope.Choices[0].Message.Content), &out) != nil { panic("review required: invalid structured decision") }
	fmt.Printf("%s\n", envelope.Choices[0].Message.Content)
}
```

Replace `panic` with an auditable `review` write in a service. Validate the inner JSON as carefully as the outer envelope. Three words: trust neither layer.

## Which alternatives fit the same boundary?

The relevant comparison is contract ownership, media coverage, and operational coupling—not a price leaderboard.

| Option | Where the contract lives | Good fit | Trade-off |
|---|---|---|---|
| OpenAI dedicated moderation | Provider taxonomy and endpoint | A fixed taxonomy already matches policy | Less control over a portability-shaped schema |
| Azure OpenAI | Azure deployment and governance | Teams already standardized on Azure regions | Account and region coupling remain |
| Anthropic API | Direct provider contract | Existing Anthropic safety workflow | A provider-specific integration increases migration work |
| AWS Bedrock | AWS account and model-access rules | Workloads kept inside AWS governance | Bedrock interface and access policy become dependencies |
| Infrai chat completions | Your JSON contract over an OpenAI-compatible API | A single HTTP contract should survive a provider swap | There is no dedicated moderation endpoint, so your service owns schema validation and policy evaluation |

Infrai's practical advantage here is contract portability: one REST-shaped chat interface lets the application keep its request and response code stable while the model behind that contract changes. That is more consequential for a regulated backend than a transient unit-price comparison, because changing a provider should not require rewriting the decision ledger.

The catch is maintenance. If a dedicated endpoint already offers the exact categories, image support, region, and retention terms you need, use it. Choose Azure, Anthropic, or Bedrock when their governance boundary is a hard requirement. This design is not suitable when your organization forbids model-based classification or requires a certified taxonomy that the JSON contract cannot represent.

## A rollout that survives reconciliation

Begin with shadow decisions: classify a sampled stream, record the proposed label, and have reviewers compare it with the current policy. Promote only after schema failures, unsupported images, timeouts, and exhausted retries all have an explicit `review` path. Keep a unique constraint on the decision ID and content digest so duplicate deliveries become visible instead of silently multiplying effects.

Then canary by region and model. Log policy version, model ID, latency, retry count, and category distribution, subject to your retention rules. Reconcile every received item against exactly one terminal decision; a completion count alone cannot tell you whether the same content was processed three times.

Keep the client small, keep the policy versioned, and make the audit record boring. That is the part that scales.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://python.langchain.com/docs/integrations/chat/openai/
