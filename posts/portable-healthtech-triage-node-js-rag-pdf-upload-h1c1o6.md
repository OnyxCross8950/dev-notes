# Portable Healthtech Triage: Node.js RAG PDF Uploads with Embeddings and pgvector

Short answer: for a healthtech support-triage system, keep PDF ingestion, chunk metadata, embeddings, semantic search, and citations behind replaceable provider interfaces; use PostgreSQL with pgvector when authorization and audit records already live there, and make every document and chunk identity deterministic so a provider change or retry does not alter the evidence trail.

This is an architecture decision record, not a recipe for choosing a fashionable model. A support ticket may mention a symptom, a claim status, and a policy question in the same paragraph. The answer needs to route the ticket correctly without exposing another patient's material, and a reviewer needs to know which page supported the decision. Provider portability therefore means more than swapping an HTTP client: the system must preserve source identity, access rules, retrieval semantics, and reviewable citations while the embedding or answer provider changes.

## A correct-looking triage answer can still fail review

The system has three independently replaceable stages. Ingestion turns an uploaded PDF into normalized text and source locations. Retrieval turns a ticket into an ordered set of authorized chunks. Answering turns that set into a draft triage suggestion. A timeout in one stage must not make the other stages appear complete.

I use four invariants for this workflow:

- A document ID comes from immutable content and tenant scope, never from a mutable filename.
- A chunk ID includes the document ID, chunking version, source page, and ordinal, so an idempotent retry converges on one row.
- Authorization is applied before vector ranking, not after the model has seen the candidates.
- A citation is rendered from stored metadata and validated against the retrieved evidence set; generated page numbers are not trusted.

The choice between a relational vector extension and a separate vector service is a boundary decision. It should be recorded explicitly because both options can be correct.

| Option | Good fit | Cost or limitation to accept |
|---|---|---|
| PostgreSQL with pgvector | Ticket state, tenant policy, source metadata, and vectors need one transactional ownership boundary | The Postgres team owns vector indexes, resource isolation, and query tuning |
| Separate vector database | Similarity traffic needs independent scaling or operational isolation | Deletion, authorization metadata, and reconciliation cross a system boundary |
| Direct provider integrations | A provider-specific embedding or generation behavior is a hard requirement | Credentials, retention review, and portability work remain separate per capability |
| A provider-neutral gateway | Several backend capabilities should share one HTTP contract and one credential boundary | Its common API cannot erase provider-specific limits; those limits still belong in the ADR |

The gateway is not the architectural center. The evidence contract is. If a gateway is used, the application should still own document manifests, chunk records, model identifiers, and audit events rather than treating a single upstream response as the source of truth.

## How should Node.js RAG preserve PDF upload evidence across providers?

The upload endpoint should create a document manifest before any embedding request. The manifest records tenant, content digest, original filename, parser version, chunking version, expected chunk count, and a state such as `received`, `indexing`, or `ready`. A file rename changes presentation metadata, not identity. A changed parser or tokenizer creates a new version and a deliberate re-index operation.

Chunking is a retrieval policy, not a cosmetic preprocessing step. A whole PDF often buries the relevant paragraph in irrelevant context; tiny fragments lose the definition that makes a policy sentence meaningful. Start with boundaries that preserve headings, list items, and page locations, then measure retrieval judgments on representative healthtech documents: benefit policies, internal escalation procedures, and de-identified ticket examples. Token limits belong to the selected tokenizer, so a word-count example is only a readable model of the identity rule.

Metadata should travel with the vector row. At minimum, retain tenant ID, document ID, filename, page, section when extraction provides one, chunk ordinal, chunking version, embedding model identifier, and the exact normalized text used for embedding. A citation record can then point to a stable chunk ID, while the renderer decides whether to display `policy.pdf, page 7` or an internal document link.

Citations are data.

For healthtech, the access predicate is part of the retrieval contract. A user who can submit a ticket is not automatically entitled to every document in the organization. Filter candidates by tenant, role, document status, and any patient or team scope before distance ordering. Keep the filter in the same query boundary as retrieval where possible, and test it with adversarial fixtures in which the semantically closest chunk is unauthorized.

## The Go contract that makes a provider replaceable

Although the production worker may be Node.js, this focused Go example makes the durable part explicit: extracted pages become deterministic, versioned records. It does not pretend that PDF parsing or embedding calls are transactions. The caller can persist the manifest and upsert these records before dispatching provider-specific work.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

type Chunk struct {
	ID       string `json:"id"`
	Tenant   string `json:"tenant"`
	Document string `json:"document"`
	Page     int    `json:"page"`
	Ordinal  int    `json:"ordinal"`
	Version  string `json:"chunking_version"`
	Text     string `json:"text"`
}

func digest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func makeChunks(tenant, documentID, filename, pageText string, page, size, overlap int) []Chunk {
	words := strings.Fields(pageText)
	step := size - overlap
	if size <= 0 || overlap < 0 || overlap >= size {
		return nil
	}

	chunks := make([]Chunk, 0)
	for start, ordinal := 0, 0; start < len(words); start, ordinal = start+step, ordinal+1 {
		end := start + size
		if end > len(words) {
			end = len(words)
		}
		text := strings.Join(words[start:end], " ")
		chunks = append(chunks, Chunk{
			ID:       digest(tenant, documentID, "chunk-v1", fmt.Sprint(page), fmt.Sprint(ordinal)),
			Tenant:   tenant,
			Document: filename,
			Page:     page,
			Ordinal:  ordinal,
			Version:  "chunk-v1",
			Text:     text,
		})
	}
	return chunks
}

func main() {
	chunks := makeChunks(
		"clinic-42",
		"document-content-digest",
		"benefits-policy.pdf",
		"Prior authorization requires two reviewers and a dated decision record.",
		7, 8, 2,
	)
	encoded, err := json.MarshalIndent(chunks, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(encoded))
}
```

The example uses words so the identity relationship is easy to inspect. A production implementation should size chunks using the tokenizer associated with the embedding model and keep that tokenizer or normalization choice in the chunking version. Do not change overlap, normalization, or page-boundary handling while retaining `chunk-v1`; the resulting vectors would no longer be comparable to the audit record that describes them.

The application should also reject invalid configuration before processing a page. A zero or negative step would loop forever, which is why the example checks `overlap >= size` rather than relying on a caller to behave. Small details like this matter in workers: an ingestion process that silently emits no chunks can look like a provider problem when the actual failure is local validation.

## Test provider migration at the retrieval boundary

At query time, embed the support ticket through an interface whose implementation can be replaced. Store the embedding model identifier and vector dimension with the index metadata. Query the authorized chunk set, order by vector distance, and cap the evidence set before constructing the answer prompt. The answer service receives citation IDs and source fields alongside text, never just an unlabelled string assembled by a helper.

The database boundary should make the authorization rule visible. A representative query is intentionally ordinary SQL:

```go
const nearestAuthorized = `
SELECT chunk_id, document_id, page, text, embedding <=> $1 AS distance
FROM document_chunks
WHERE tenant_id = $2
  AND status = 'ready'
  AND can_read_document(document_id, $3)
ORDER BY embedding <=> $1
LIMIT $4`
```

The exact operator and index configuration belong to the pgvector version and deployment, so they should be tested against the installed extension rather than copied into a provider abstraction as universal behavior. The portable contract is the returned identity, metadata, text, and ordering, not an assumption that every vector store uses the same query language.

Retries need the same care as a ledger posting. Suppose the manifest contains 120 chunks, 93 embeddings have been committed, and a provider call returns HTTP 429. My rule is to reconcile the manifest first, honor the provider's retry signal when available, and submit only IDs without a committed vector. A second worker may receive the same job, but a unique key on `(tenant_id, chunk_id, embedding_model, chunking_version)` keeps its logical effect singular. A 429 is an operational state to record, not evidence that a chunk should be duplicated.

The answer itself is not mathematically exactly-once; model generation is probabilistic. What can be exactly-once is the ingestion effect and the audit event: one document version, one chunk identity, one evidence-set digest, and one recorded answer attempt. Persist the query, authorized scope, ordered chunk IDs, model identifiers, prompt or template version, response, and reviewer outcome according to the applicable retention policy. These records let an operator explain a routing decision after the fact without claiming that a remote model call participated in the database transaction.

I'm not sure a universal chunk size exists across prior-authorization policies and escalation playbooks. Your mileage may vary. Versioned fixtures and retrieval judgments can resolve that uncertainty; a confident constant cannot.

## Where this design is deliberately the wrong tool

The rejected option is to place the entire uploaded PDF into every answer prompt. It is valid for a small prototype with short, non-sensitive documents when the team is testing task usefulness rather than operating a multi-tenant index. It is not suitable when source authorization, repeatable citations, bounded context, or deletion verification matters. Ordinary keyword search is also the better tool for exact ticket IDs, claim codes, and document titles; semantic search should not replace a precise lookup path.

Provider portability has a boundary too. A shared HTTP interface can normalize request lifecycle, timeout handling, model identifiers, and audit fields, but it cannot make embedding dimensions, tokenizer behavior, retention terms, regional availability, or moderation controls identical. Keep those differences in capability metadata and contract tests. Stick with a direct integration when a provider-specific behavior is a hard requirement or when the gateway's common surface cannot express the required control.

Compliance review must remain concrete. For regulated or sensitive material, verify retention, residency, subprocessors, deletion guarantees, access logging, and export requirements for the actual deployment and account. API compatibility is not compliance equivalence. A system that stores patient-adjacent text in an embedding index still needs a deletion state machine: remove the document from eligible retrieval, retire or delete its expected chunk IDs, verify the count, and preserve only the audit event permitted by policy.

That is the decision rule: choose the storage and provider arrangement that preserves the evidence boundary under replacement, retries, deletion, and review. The model is a component. The audit trail is the product constraint.

## Further reading

- OpenAI, “Embeddings guide”: https://platform.openai.com/docs/guides/embeddings
- pgvector, “Open-source vector similarity search for Postgres”: https://github.com/pgvector/pgvector
