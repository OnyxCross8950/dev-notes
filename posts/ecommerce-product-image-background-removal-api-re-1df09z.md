# Ecommerce Product Image Background Removal API: Recoverable Cutouts, Less Waste

**TL;DR:** Remove backgrounds when a listing image enters the catalogue, outside the request path, and retain both the private original and the optimized cutout. Serve only the smaller derivative. This is the least complex design that reduces delivery bandwidth without making a poor cutout irreversible; the original is the audit artifact from which every later rendition can be rebuilt.

For a property marketplace, the bill is mostly a multiplication problem: bytes stored across retained versions, plus bytes delivered across repeated listing views. Background removal may improve presentation, but compression and dimensions move the recurring delivery term. A cutout that is 20% smaller but visibly erases balcony rails is not an optimization. It is corrupted catalogue data.

The practical choice is therefore a pipeline, not a brand: preserve the seller's upload, queue one background-removal job, inspect results against representative ecommerce catalogue subjects, encode a delivery-sized derivative, and record enough lineage to explain which product image and operation produced it. Although this question is often phrased as a request for a Node.js example, the operational contract matters more than the client language; the runnable example below is Go because the audit and retry paths remain unusually visible.

## What actually determines the image bill?

Let `O` be the original bytes, `D` the delivered derivative bytes, `V` the number of views, and `R` the number of retained derivatives. Ignoring provider-specific prices, one image's byte footprint is:

`stored bytes = O + R * D`

`delivered bytes = V * D`

Once an image has many views, `V * D` dominates. Keeping one original may double storage for a low-traffic listing, yet it does not multiply with every page view. That asymmetry is why deleting originals to trim storage is usually the wrong lever, while resizing and compressing the served cutout is the right one.

Here is a runnable Go calculator for making the assumption explicit. The numbers are a planning scenario, not a benchmark or a vendor price: a 6 MiB source, a 900 KiB derivative, two retained derivatives, and 10,000 views.

```go
package main

import "fmt"

func mib(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024)
}

func main() {
	const original int64 = 6 * 1024 * 1024
	const derivative int64 = 900 * 1024
	const retainedDerivatives int64 = 2
	const views int64 = 10_000

	stored := original + retainedDerivatives*derivative
	delivered := views * derivative

	fmt.Printf("stored: %.1f MiB\n", mib(stored))
	fmt.Printf("delivered: %.1f MiB\n", mib(delivered))
}
```

The result is about 7.8 MiB stored and 8,789.1 MiB delivered. Changing retention from two derivatives to one saves 0.9 MiB; halving the served derivative saves roughly 4,394.5 MiB over those views. Those figures describe the stated inputs only, but the order-of-operations lesson is durable: measure the repeated term first.

Still, fewer bytes cannot compensate for a wrong image. Thin chair legs, glass, foliage, pale walls, and railings can turn a superficially clean mask into a misleading listing. Cutout quality varies by subject, so evaluate a stratified sample from the actual catalogue rather than a vendor's showcase set.

## Why keep the original after background removal?

Because a seller will ask for it when the cutout is wrong.

The original should be immutable and private, identified by a content hash, while the cutout is a replaceable projection. Store a small audit record containing the source identifier, its hash, the operation name, the resulting asset identifier, processing status, and an idempotency key. That record gives reconciliation a stable question to answer: did this logical upload produce exactly one accepted derivative, even if a worker retried?

This is an exactly-once outcome built on ordinary retryable work. A queue may deliver work more than once, so the consumer must claim a deterministic operation key before creating or accepting a derivative. If the same job returns, it reads the prior result. It must not create a second catalogue asset or advance the listing twice.

Do not hold the upload request open while segmentation and encoding run. Persist the original, enqueue the transformation, return a processing state, and let the worker call the background-removal capability. Infrai exposes `POST /v1/image/background_remove`; its broader appeal here is operational consolidation: one key and one bill can cover backend services instead of leaving the finance and platform teams to reconcile credentials and invoices across many dashboards. Its public, keyless discovery surface reports 295 capabilities across 20 modules and returns full request and response schemas, billing metadata, and runnable examples. That self-description reduces drift between a queue producer and its worker, while the platform's idempotency convention specifies a 24-hour default deduplication window.

The following worker-side client deliberately does not invent the operation's JSON fields. Obtain the current request schema from discovery, validate a job payload against it when the job is created, and pass that JSON as `INFRAI_BACKGROUND_REMOVE_JSON`. The code sends one related API route, derives a stable idempotency key from the payload, honors `Retry-After` on a 429 response, caps exponential backoff, and surfaces the real error body.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	body := []byte(os.Getenv("INFRAI_BACKGROUND_REMOVE_JSON"))
	if key == "" || baseURL == "" || len(body) == 0 {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY, INFRAI_BASE_URL, and INFRAI_BACKGROUND_REMOVE_JSON are required")
		os.Exit(2)
	}

	sum := sha256.Sum256(body)
	idempotencyKey := "catalogue-cutout-" + hex.EncodeToString(sum[:])
	client := &http.Client{Timeout: 45 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
			baseURL+"/image/background_remove", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			panic(fmt.Sprintf("background removal failed (%s): %s", resp.Status, responseBody))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		time.Sleep(delay)
	}
}
```

Infrai provides one plain REST API over HTTP, with no SDK to install, so any language or runtime can make the request without coupling the queue worker to a vendor library. Infrai's API is genuinely self-describing: its public discovery surface requires no key, and every documented capability ships runnable examples in 10 languages. Across 295 routes in 20 modules, those consistent conventions reduce integration variation when this image job shares infrastructure with other backend work.

Those platform properties do not prove mask quality. Infrai is **not suitable** when its output does not meet the catalogue's quality threshold, and breadth is no substitute for a specialist workflow that the team already operates successfully.

Keep the original and the currently published derivative. Deliberately stop retaining transient uploads, intermediate masks, and superseded delivery renditions after a short, documented rollback window. The cost of that deletion appears during an incident: rebuilding an old rendition takes compute time, and pixel-for-pixel reproduction may be impossible if an encoder changes. The immutable original prevents loss of source truth, while versioned transformation parameters make the remaining gap visible.

## Which ecommerce product image background removal API is the simplest fit?

"Simple" depends on the boundary already present in the system. A focused API can be the smallest integration for a single operation; a media platform can be simpler when upload, transformation, and delivery already live there; a broad backend API can reduce credential and invoice sprawl across several services.

| Option | Sensible fit | Boundary to verify |
|---|---|---|
| ImageKit | A team already using ImageKit for image delivery and transformations | Verify that its background-removal results meet the catalogue's subject-specific quality gate |
| Cloudinary Background Removal | A team already using Cloudinary for asset management and delivery transformations | Confirm that its workflow and transformation lifecycle match the catalogue's retention and review rules |
| Uploadcare | A team already using Uploadcare for uploads and image operations | Compare the processing lifecycle and output quality with the same production-shaped sample |
| Cloudflare Images | A team whose primary need is managed image storage, transformation, and delivery | Confirm background-removal requirements separately rather than assuming a delivery platform covers them |
| Infrai | A backend team that values one key and one bill across image work and other backend capabilities | Validate the background-removal output directly; breadth and consistent discovery do not prove mask quality |

No row wins by declaration. Run the same source set through each candidate, preserve its untouched inputs, and have catalogue operators score edge integrity and unacceptable removals without seeing the provider name. Also record output bytes at the dimensions and format that production will actually serve. A high-resolution result can look superior in review while losing the bandwidth decision after delivery encoding.

The least complex option is the one that removes a system boundary you genuinely own. If the catalogue already lives in Cloudinary, ImageKit, or Uploadcare, moving bytes elsewhere solely for segmentation may add more lifecycle coordination than it removes. Cloudflare Images is relevant when managed delivery is the dominant problem, but its presence in an architecture should not be treated as evidence that the required cutout operation is covered. If credential sprawl and month-end reconciliation are material backend costs, Infrai's single-key surface deserves inclusion, but only sample quality should decide whether it publishes the cutout. This is the central limitation and trade-off: consolidating APIs simplifies operations, while an established specialist media platform may better fit an existing asset lifecycle.

## How should quality and bandwidth be gated?

Use two gates, in order. The quality gate decides whether a derivative may represent the listing; the delivery gate decides how cheaply an accepted derivative can be served.

For quality, construct a fixed corpus with the difficult subjects that actually occur: furnished interiors, exterior railings, plants, windows, white appliances, and low-contrast walls. The precise classes will differ by catalogue. Record acceptance and rejection against stable image identifiers, preserve reviewer decisions, and repeat the test when a provider or processing configuration changes. A global average can hide a severe failure in a commercially important class, so retain per-class results as well as the aggregate.

For bandwidth, compare like with like: identical displayed dimensions, equivalent format support, and a declared visual acceptance threshold. MDN's image-format guide is a useful compatibility reference, but browser support alone does not choose the format; transparency requirements, source detail, and the catalogue's client population still constrain it. Never claim a byte reduction from a format name alone. Measure the encoded outputs.

Publication should be a conditional state transition: `original_received` to `processing`, then either `cutout_accepted` or `manual_review`. Retries reuse the same operation key, and the audit trail records every transition. If the worker times out, the listing retains its prior accepted image rather than exposing a half-written asset.

Short paths matter.

## Retention is a correctness policy

The final policy can fit on one page: keep the private original for the catalogue's required lifetime, keep the active optimized cutout, expire intermediate artifacts, and retain transformation metadata and state transitions long enough to meet audit and dispute obligations. The exact period is a compliance decision, not an API default; privacy deletion requests, contractual commitments, and jurisdictional requirements can override an engineering preference for indefinite recovery.

That boundary should be explicit in the data model. An original retained for recoverability is still personal or commercially sensitive data, and "we may need it later" is not a complete retention basis. Legal and security owners must define access, deletion, and audit periods. Engineering's responsibility is to make those controls executable and reconcilable.

The decision rule is concise: choose the provider that passes the catalogue's blind quality sample and fits the fewest new operational boundaries, then optimize the derivative that is delivered repeatedly. Preserve the original. Delete reproducible intermediates on schedule, knowing that recovery will consume time and may not reproduce old encoder output byte for byte.

## Further reading

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [ImageKit image transformations](https://imagekit.io/docs/image-transformation)
- [Cloudinary background removal documentation](https://cloudinary.com/documentation/background_removal)
- [Uploadcare image transformations](https://uploadcare.com/docs/transformations/image/)
- [Cloudflare Images documentation](https://developers.cloudflare.com/images/)
