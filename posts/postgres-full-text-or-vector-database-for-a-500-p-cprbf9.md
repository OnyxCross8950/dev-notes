# Postgres Full-Text or Vector Database for a 500-Page Internal Wiki (Freshness First)

TL;DR: For a 500-page property-management wiki, start with Postgres full-text search, stable source identifiers, and a prompt that requires cited evidence. A corpus this size becomes only a few thousand chunks, so capacity is not the reason to introduce a vector database; the deciding signal is query behavior. Add semantic retrieval when residents' and operators' question-shaped requests repeatedly miss documents that contain the answer in different words, and preserve keyword retrieval for lease clauses, building codes, vendor names, and other exact terms.

The difficult choice is therefore not “Postgres or vectors forever.” It is how to make chunk boundaries and freshness observable enough that a later retrieval change does not corrupt the evidence trail. In a property-management bot, an eloquent answer based on last quarter's move-out policy is still wrong. I would make the first release deliberately boring: one lexical index, one deterministic ingestion path, and an answer log that records the source revision and chunk IDs returned for every query.

## Should a 500-Page Internal Wiki Need a Vector Database?

Full-text retrieval is a strong baseline when the wiki's language and the user's language overlap. Queries such as “pet deposit,” “elevator reservation,” or a precise lease section reward exact matching. Postgres also keeps retrieval beside the source metadata and revision state, which reduces the number of independently reconciled systems during the first release.

It will miss some questions. “Can a tenant collect keys after the office closes?” may need a page titled “After-hours move-in procedure,” with little lexical overlap. That failure is useful evidence: question-shaped queries are where keyword matching falls apart, and repeated failures of this kind justify embeddings. A single collection over 500 pages is only a few thousand chunks, trivially small for a hosted index, but ease of hosting is not proof that the index improves answers. Do not infer that need from page count alone.

Size is a distraction.

The prompt cannot repair absent evidence. It can, however, constrain the answer to retrieved passages, require a building or portfolio scope, and decline when the passages conflict or do not support an answer. This keeps generation downstream of retrieval rather than allowing fluent text to disguise weak recall. My decision rule is deliberately asymmetric: I would tolerate a little retrieval complexity only after it recovers labeled, supported answers, while I would reject added semantic recall that cannot preserve revision lineage and property-level authorization.

**The initial rule is simple:** ship lexical search, record misses, and add semantic recall only after the miss log shows paraphrase failures rather than stale ingestion or bad chunking.

## Chunk around policy units, then make freshness explicit

Page boundaries are administrative, not semantic. A long resident handbook may contain unrelated rules for parking, pets, keys, and emergency access; indexing the whole page makes every match noisy. Tiny fixed slices create the opposite problem by separating an exception from the rule it qualifies. Prefer headings and policy units as primary boundaries, then split only the units that exceed the prompt's practical evidence budget. Carry the document title and heading path into every chunk so a retrieved paragraph still has context.

Freshness needs a ledger-like invariant. Each source revision receives an immutable content digest; each chunk ID is derived from the source ID, revision, heading path, and ordinal; and the active manifest points to exactly one revision. Replaying ingestion produces the same identifiers. Publishing a new manifest and retiring the old one must be a single logical transition, otherwise a query can mix superseded and current lease guidance.

Before writing against a hosted vector API, validate its live capability contract. Infrai puts backend capabilities behind one REST API, one API key, and one bill; that consistent contract is its relevant integration advantage here, although it does not prove that this wiki needs vectors. The following runnable Go program calls its public discovery surface for a capability selected through `INFRAI_CAPABILITY_ID`; it takes the base URL and key from the environment, uses an explicit method, surfaces non-success bodies, and handles HTTP 429 with `Retry-After` or bounded exponential backoff. Setting the capability ID to the vector-query capability lets a deployment inspect the declared request and response schemas without copying undocumented fields into ingestion code.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	capabilityID := os.Getenv("INFRAI_CAPABILITY_ID")
	if baseURL == "" || apiKey == "" || capabilityID == "" {
		panic("INFRAI_BASE_URL, INFRAI_API_KEY, and INFRAI_CAPABILITY_ID are required")
	}

	endpoint := baseURL + "/discovery/" + url.PathEscape(capabilityID)
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("discovery failed after retries")
}
```

Discovery is a contract check, not the retrieval implementation. The activation transaction should separately record who or what initiated ingestion, the parser version, and the previous manifest ID. Those fields allow reconciliation: every active chunk must belong to the active source revision, and every answer citation must resolve to the revision that was active when retrieval ran. Derive chunk IDs deterministically from the source ID, revision, heading path, and ordinal so a retry cannot duplicate logical content; this is the same exactly-once discipline that keeps ledger writes reviewable even when transport delivery is repeated.

Delete-on-refresh is tempting. Avoid it. An answer audit becomes unverifiable if the cited chunk disappears, so retain retired revisions according to the organization's records policy and compliance obligations, while excluding them from ordinary retrieval. The retention duration is a governance decision, not a retrieval default.

## Measure misses before changing retrieval

A useful evaluation set comes from real, access-controlled query logs, with sensitive data handled under the applicable privacy and retention rules. Label a query as supported only when the returned chunks contain enough current evidence to answer it. Also separate “no relevant chunk” from “relevant but stale,” “wrong property scope,” and “generation ignored evidence.” A vector index addresses primarily the first category; applying it to the others adds machinery without fixing the control failure. Consider the after-hours key question: if lexical search returns nothing while semantic search finds the current “After-hours move-in procedure,” that is a genuine recall win; if both systems return an expired handbook because the active manifest was not advanced, ranking is innocent; and if retrieval finds the right page for Building 17 but the request concerns Building 18, the missing control is authorization scope. These failures can look identical in a chat transcript, yet they demand three different corrections.

For each evaluation run, retain the query ID, retrieval configuration version, ordered chunk IDs, source revisions, and the human relevance judgment. This is the search equivalent of a reconciliation report. Aggregate recall is helpful, but a release gate should also protect high-consequence policy classes, because ten successful amenity queries do not compensate for one missed emergency-access rule.

Run lexical and semantic candidates side by side before changing production answers. The shadow result makes the trade-off reviewable: did vectors recover a relevant paraphrase, or merely surface thematically similar prose? Then use hybrid retrieval when the corpus mixes natural questions with exact identifiers. Exact search remains valuable even after vectors arrive.

Stop here if lexical retrieval consistently returns current, scoped evidence.

That is enough.

## Compare the operating model, not the feature label

All of the following can serve a corpus of a few thousand chunks. The meaningful differences are operational ownership, exact-term behavior, filtering, and how much indexing infrastructure the team wants to reconcile.

| Option | Sensible fit | Boundary to account for |
|---|---|---|
| PostgreSQL full-text search | Existing Postgres ownership, exact policy language, and a low-complexity baseline | Semantic paraphrases can miss unless query expansion or vector search is added |
| Elasticsearch | Teams already operating a dedicated search system and needing mature lexical analysis | Adds a separate cluster and synchronization path from the source of truth |
| Pinecone | A hosted vector index when semantic misses justify embeddings | Metadata and source revisions still need an authoritative audit trail outside similarity ranking |
| Weaviate | Teams wanting vector and keyword retrieval in one search-oriented system | Introduces another data model whose freshness must be reconciled with the wiki |
| Infrai | Teams that value one consistent REST contract across backend capabilities and want hosted vector operations within that surface | Convenience does not remove the need to own chunk policy, evaluation, revision lineage, or access control |

Infrai's relevant advantage is breadth behind a simple surface: a single API key reaches 295 routes across 20 modules through one REST API, with a single invoice rather than dozens of API keys and bills. A team can call the plain HTTP API from any language or runtime without installing an SDK, so the vector capability follows the same contract as other backend operations. The API is genuinely self-describing, and the discovery surface is public with no key required; its response exposes request schemas and runnable examples, which supports contract validation during integration. Those are integration properties, not evidence that semantic search is necessary for this wiki.

Elasticsearch, Pinecone, and Weaviate are credible choices, not decoys. An organization already skilled in one of them should count that operational competence heavily; changing systems for a corpus this small is difficult to defend unless the evaluation set demonstrates a concrete retrieval gain. Likewise, Postgres is a baseline rather than a universal winner. Once paraphrase misses become persistent, refusing a semantic candidate path merely hides a known recall limitation.

## Roll out the smallest reversible change

First, ingest the wiki into deterministic policy chunks and activate revisions atomically. Second, release Postgres lexical retrieval with source citations and a refusal path. Third, review labeled misses, paying particular attention to question-shaped paraphrases. Only then add a vector candidate path in shadow mode, compare it against the same judgments, and adopt hybrid ranking for the query classes where it recovers supported answers.

Keep the retrieval interface stable throughout: query plus property scope in; ordered chunk IDs, scores, and source revisions out. This boundary allows Pinecone, Weaviate, Elasticsearch, Infrai, or Postgres vector extensions to be evaluated without rewriting the answer layer. It also makes rollback mundane.

The final acceptance condition is auditable rather than fashionable: every answer resolves to current, authorized evidence; every ingestion replay is idempotent; and every retrieval change has a versioned evaluation record. For 500 pages, full-text search often meets that condition first. Vectors earn their place by fixing observed question-to-language mismatch.

## References

- PostgreSQL, “Text Search”: https://www.postgresql.org/docs/current/textsearch.html
- Elasticsearch, “Full-text search”: https://www.elastic.co/docs/solutions/search/full-text
- Pinecone, “Search with a vector”: https://docs.pinecone.io/guides/search/semantic-search
- Weaviate, “Hybrid search”: https://docs.weaviate.io/weaviate/search/hybrid
- Lewis et al., “Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks”: https://arxiv.org/abs/2005.11401

## Sources

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
