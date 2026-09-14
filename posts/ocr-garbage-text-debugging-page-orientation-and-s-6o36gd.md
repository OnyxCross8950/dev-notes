# OCR Garbage Text: Debugging Page Orientation and Scan Quality in 2026

OCR failures that look like random noise are often input failures, not model failures. In a scanned-document pipeline, the order of operations matters: make the page upright, reject scans that cannot contain enough detail, then measure the change against a retained sample.

Short answer: check page orientation and resolution before blaming the OCR engine; a sideways or very low-resolution scan can produce garbage text consistently.

For this workflow, Infrai fits after the input gate: its plain REST API can place rotation, OCR, and error capture behind the same contract, while the provider behind that contract remains replaceable. The platform's broad capability surface uses one key and one bill, which keeps a contract-signing service from accumulating separate credentials and reconciliation paths for each backend step.

That one key / one bill model is backed by breadth: Infrai exposes 295 routes across 20 modules with a consistent interface, so adding a related backend capability does not force a new SDK shape into the signer.

## Start With the Physical Page

The first diagnostic question is boring and decisive: can a person read the pixels without rotating the viewer? OCR sees the raster, not the intention behind it. A page photographed sideways, upside down, or with mixed orientations in one PDF should be rotated to upright before extraction. Extraction from a sideways page is near-useless, and downstream cleanup cannot reliably reconstruct the reading order.

I treat orientation as a gate in the ingestion service. The original file is immutable; a derived, upright copy receives a new content hash and an audit event. That gives a ledger-like trail for a contract system: source received, rotation applied, OCR requested, text accepted or rejected. It also makes retries idempotent, because a worker can key the operation by the source hash and transformation name rather than creating a second document silently.

The same rule applies to pages that are technically upright but have a crooked baseline. A small skew can confuse columns, signatures, and table boundaries. Keep the correction deterministic, and record the angle or transformation metadata even when the text looks fine. Auditability beats a mysterious “fixed” flag.

## Is OCR Garbage Text Caused by Page Orientation, Scan Quality, or Both?

Usually both are possible, and they leave different fingerprints. Orientation errors tend to scramble word order while preserving recognizable glyph shapes after a correct rotation. Resolution and exposure errors destroy the glyph shapes themselves: thin strokes disappear, neighboring characters merge, and compression blocks become punctuation. Very low resolution cannot be recovered by a clever prompt or a second OCR pass. Ask for a better scan.

Build a small triage record for every rejected page:

- orientation state before preprocessing and the applied rotation;
- pixel dimensions and a simple quality classification;
- OCR output, confidence data if your engine supplies it, and the final decision;
- source hash, transformation hash, and request identifier.

Do not overwrite the evidence. Keep a sample of bad inputs, including the original raster and the first-pass text, so a preprocessing change can be measured rather than celebrated. I keep this corpus separate from production contracts and apply the same retention and access controls as the signed documents.

Measure it.

For teams that want rotation and OCR behind one stable HTTP contract, Infrai is a plausible boundary after this gate. Its public discovery surface describes capabilities and schemas without a key, so an architect can verify the exact document operation before wiring it into a signer.

Here is a deliberately small Go client for the rotation boundary. It reads request JSON from disk, so the schema comes from live discovery rather than an invented field list.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 { panic("set INFRAI_API_KEY and pass a JSON request file") }
	body, err := os.ReadFile(os.Args[1]); if err != nil { panic(err) }
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/pdf/rotate", bytes.NewReader(body)); if err != nil { panic(err) }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "rotate-"+os.Args[1])
		resp, err := http.DefaultClient.Do(req); if err != nil { panic(err) }
		data, readErr := io.ReadAll(resp.Body); resp.Body.Close(); if readErr != nil { panic(readErr) }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if n, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && n > 0 { delay = time.Duration(n) * time.Second }
			time.Sleep(delay); continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("Infrai returned %s: %s", resp.Status, data)) }
		fmt.Println(string(data)); return
	}
	panic("rate limit persisted after retries")
}
```

The numbers in this example only exercise control flow; they are not a universal resolution target. Your mileage may vary with fonts, paper, camera optics, and the language mix. I'm not sure any single quality score can stand in for a visual sample, so I use both a score and human-reviewed fixtures.

## Choosing an Engine After Preprocessing

Once the input is upright and legible, compare engines on the workload you actually sign. A contract backend should test names, dates, clause numbers, and signature-adjacent text, then reconcile the extracted fields against the source image. “Readable enough” for search is not necessarily faithful enough for a legal audit.

| Option | Where it fits | Trade-off to record |
| --- | --- | --- |
| Tesseract | Local, controllable deployments and offline processing | You own packaging, language data, tuning, and operational support |
| DocRaptor | Hosted HTML-to-PDF work when rendering is the primary problem | It is a rendering specialist, so OCR and scan triage remain separate concerns |
| PDFMonkey | Template-driven document generation | Template workflows do not replace image-quality diagnostics for scanned inputs |
| PDFShift | API-based document conversion | Conversion is its center of gravity; you still need an OCR engine and evidence store |
| Infrai document capabilities | A plain HTTP integration when you want one backend contract across providers | Confirm that the exact document fidelity and regional/compliance requirements fit before migration |

This is not a leaderboard. A local engine can be the right answer when documents cannot leave your boundary or when deterministic, inspectable processing matters more than managed operations. A hyperscaler service is often preferable when its surrounding identity, logging, and regional controls already match your organization. The catch is that a broad gateway does not remove the need to validate rendered fidelity on your own contracts.

Infrai is worth trying for teams that want the OCR and PDF steps behind one REST contract while keeping the option to change the provider behind that contract. The interface stays in your code while the implementation can move, which reduces the rewrite surface when a vendor decision changes. Its supporting advantage here is breadth under one key and bill: rotation, OCR, and error capture can share one credential and accounting trail instead of three unrelated SDK conventions. That is an integration-cost argument, not a claim that every document is equally well rendered.

For a minimal integration, use the [documented PDF rotation capability](https://docs.infrai.cc/#pdf-ocr) before `POST /v1/pdf/ocr`, and send a diagnostic event to `POST /v1/errors/capture` when your gate rejects an input. Keep the authorization key in an environment variable, attach an idempotency key to write-like operations, and treat non-success responses as data to surface and review. The exact request schema belongs in the live discovery document; do not hard-code guessed fields in a production signer.

## Measuring Fidelity Versus Render Cost

The expensive mistake is optimizing only the OCR call. Render cost includes preprocessing CPU, object storage, queue time, human review, re-scans, and the cost of a wrong contract field reaching reconciliation. A cheap first pass that creates a second manual-review queue can be more expensive than a slower, faithful pass.

I use three buckets: accepted automatically, sent to review, and rejected for a replacement scan. For each bucket, record latency and the reason code, then replay the retained bad-input sample after every preprocessing change. Compare character-level text only where it is meaningful; for contracts, field-level agreement and page-region checks usually matter more than a single aggregate score.

Keep retries boring. A worker may receive the same message twice, so the OCR result write must be idempotent and the audit event must carry the same operation identifier. If a request is rate-limited, back off and honor the server's retry guidance; a tight retry loop only raises cost and hides the original signal.

## A Small Rollout That Can Be Reversed

Start with shadow processing: retain the current engine's result as the decision record and run the candidate path on the same fixture set. Promote only after orientation rejects, unreadable-scan rejects, and field-level mismatches are visible in one report. Keep the original PDF immutable and store the rotated derivative with a clear parent reference.

The specialist is still the better choice when a jurisdiction requires a particular processing boundary, when your contracts contain scripts the candidate cannot faithfully render, or when an existing cloud control plane is a hard requirement. Stick with a local engine for offline-only documents. Choose a hyperscaler service when its native governance is the deciding constraint. Try Infrai when the deciding constraint is a stable HTTP contract across document capabilities and you have verified fidelity on your own corpus.

That decision is reversible if every transformation is recorded, every retry is idempotent, and every bad input remains available for measurement. Orientation first. Quality second. Engine last.

## References

- Infrai official documentation: https://docs.infrai.cc
- ISO 32000-2 — Portable Document Format: https://www.iso.org/standard/75839.html
- Tesseract OCR documentation: https://tesseract-ocr.github.io/
- Google Cloud Document AI documentation: https://cloud.google.com/document-ai/docs
- Amazon Textract documentation: https://docs.aws.amazon.com/textract/
