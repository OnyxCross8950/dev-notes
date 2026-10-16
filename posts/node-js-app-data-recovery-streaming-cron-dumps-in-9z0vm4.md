# Node.js App Data Recovery: Streaming Cron Dumps into Private Object Buckets

For e-commerce app data backups, a Node.js cron upload of each database dump to private object storage creates a throughput constraint: the scheduler must not buffer the archive, overlap another run, or publish a permanent download link.

Short answer: let Node.js cron enqueue one backup run, stream a compressed database dump from a worker into a dedicated private S3-compatible bucket, and restore only through a short-lived presigned download; choose the storage service by measured end-to-end upload and restore throughput, then keep an external immutable copy when audit rules require recovery from deletion or overwrite.

The important boundary is the completed, verifiable archive, not the cron expression. A scheduler can report success while bytes are still moving, and an object can exist while the application has no trustworthy record of which database snapshot it contains. For a customer-facing report system, I would therefore make the backup ledger explicit: run ID, source snapshot identifier, object key, byte count, digest, start time, completion time, and restore-test result. It's the same discipline used for money movement: retries may happen, but the durable effect and its audit trail must remain singular.

## How much object storage throughput should a Node.js cron dump sustain?

Keep cron boring. It should create or enqueue a deterministic run ID such as `orders-2026-08-20`, refuse a second active run for that ID, and hand the work to a worker with a timeout longer than the likely transfer. If a platform scheduler has a 900-second execution ceiling, the scheduled handler should only enqueue; a worker owns compression, upload, and verification. A standard queue is at-least-once, so the consumer must use the run ID and destination key idempotently rather than assuming one delivery.

The worker should stream rather than read the archive into memory. A useful key shape is `backups/app/2026/08/20/orders.sql.gz`: the date prefix supports prefix listing and retention, while the stable final component prevents a retry from creating a second logical backup. Consider what happens when a delayed queue delivery arrives after its replacement has started: both consumers derive the same key, both can open a valid compressed dump, and both can issue a write, yet neither can attach an `If-Match` precondition. The object name alone prevents duplicate names, not a late writer from replacing the intended bytes. The application database must grant one lease for the run ID, bind that lease to the source snapshot and digest, and reject completion from a worker that no longer owns it. This is where an exactly-once mindset helps without pretending the queue is exactly-once: delivery remains repeatable, while the ledger permits one authoritative transition from prepared to uploaded. A queue-level lease can serve the same purpose if its loss and renewal semantics are part of the audit record.

One run, one effect.

There is one subtle distinction here — compression throughput and network throughput are separate budgets. If gzip produces 80 MB/s but the object path sustains 25 MB/s, adding compression workers won't shorten the critical path; if the relationship is reversed, a faster storage vendor will not help. I'm not sure which service wins for a particular production dataset without a benchmark using its real archive size, region, compression level, and restore destination. A timed upload followed by a timed restore of representative data resolves that uncertainty.

Don't mark the ledger row complete merely because the upload request returned successfully. Record the expected byte count and digest before sending, confirm the remote object metadata where the selected service exposes it, and periodically restore into an isolated database.

Restore drills are evidence.

## Make the byte stream the unit of work

The following worker is intentionally small enough to sit behind a Node.js cron or queue trigger. It accepts an already compressed dump, streams it without loading the file into memory, retries HTTP 429 with `Retry-After` or exponential delay, uses the same object key on every attempt, and asks for a signed restore response. It uses exactly two storage routes. The caller stores the returned JSON beside the backup ledger and uses the signed URL described by that response without attaching the Infrai authorization header to the download request.

```go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func escapedPath(value string) string {
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = url.PathEscape(parts[i])
	}
	return strings.Join(parts, "/")
}

func storagePath(template, bucket, key string) string {
	path := strings.ReplaceAll(template, "{bucket}", url.PathEscape(bucket))
	return strings.ReplaceAll(path, "{key}", escapedPath(key))
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if raw := response.Header.Get("Retry-After"); raw != "" {
		if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func doWithRateLimit(client *http.Client, build func() (*http.Request, error)) (*http.Response, error) {
	for attempt := 0; attempt < 5; attempt++ {
		request, err := build()
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusTooManyRequests {
			return response, nil
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		time.Sleep(retryDelay(response, attempt))
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: backup-worker <bucket> <key> <archive.sql.gz>")
		os.Exit(2)
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	apiOrigin := strings.TrimRight(os.Getenv("STORAGE_API_ORIGIN"), "/")
	if apiOrigin == "" {
		fmt.Fprintln(os.Stderr, "STORAGE_API_ORIGIN is required")
		os.Exit(2)
	}
	bucket, key, archive := os.Args[1], os.Args[2], os.Args[3]
	client := &http.Client{Timeout: 30 * time.Minute}

	uploadURL := apiOrigin + storagePath("/v1/storage/object/put/{bucket}/{key}", bucket, key)
	upload, err := doWithRateLimit(client, func() (*http.Request, error) {
		file, openErr := os.Open(archive)
		if openErr != nil {
			return nil, openErr
		}
		request, requestErr := http.NewRequest(http.MethodPut, uploadURL, file)
		if requestErr != nil {
			file.Close()
			return nil, requestErr
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Content-Type", "application/gzip")
		return request, nil
	})
	if err != nil {
		panic(err)
	}
	defer upload.Body.Close()
	if upload.StatusCode < 200 || upload.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(upload.Body, 1<<20))
		panic(fmt.Errorf("upload rejected: status=%d body=%s", upload.StatusCode, body))
	}
	io.Copy(io.Discard, upload.Body)

	presignURL := apiOrigin + storagePath("/v1/storage/object/presign/{bucket}/{key}", bucket, key)
	presign, err := doWithRateLimit(client, func() (*http.Request, error) {
		request, requestErr := http.NewRequest(http.MethodPost, presignURL, bytes.NewReader(nil))
		if requestErr != nil {
			return nil, requestErr
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)
		request.Header.Set("Content-Type", "application/json")
		return request, nil
	})
	if err != nil {
		panic(err)
	}
	defer presign.Body.Close()
	if presign.StatusCode < 200 || presign.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(presign.Body, 1<<20))
		panic(fmt.Errorf("presign rejected: status=%d body=%s", presign.StatusCode, body))
	}
	var result map[string]any
	if err := json.NewDecoder(presign.Body).Decode(&result); err != nil {
		panic(err)
	}
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(output))
}
```

Run it with a private bucket and a key produced from the ledger's run date:

```bash
go run . ecommerce-backups backups/app/2026/08/20/orders.sql.gz ./orders.sql.gz
```

For files large enough that one request cannot reliably finish inside the transfer budget, use the provider's multipart workflow and persist the upload ID plus completed part numbers in the ledger. Do not improvise route names from S3 conventions; discover the selected API contract and generate the client path from its declared `path` field. Multipart state also needs an explicit cleanup procedure because unfinished fragments do not have an automatic lifecycle cleanup rule here.

## Which provider should carry private app data backups?

The comparison should follow the constraint, not brand familiarity. Measure sustained upload and restore throughput in the deployment region, but also decide who must own provider-specific controls. No paper feature matrix can substitute for a representative archive test.

| Option | Strong fit | The catch |
| --- | --- | --- |
| AWS S3 | Teams that want a direct S3 relationship and can evaluate lifecycle controls against the primary S3 documentation | It adds a provider-specific account, key, SDK or HTTP contract, and invoice to the operating model |
| Cloudflare R2 | Teams already standardized on R2 or wanting direct control of that provider | Direct adoption preserves a separate credential and billing boundary; validate large-file throughput from the actual worker region |
| Firebase Cloud Storage | Applications already centered on Firebase's storage and client integration model | It is a separate platform choice rather than part of this API's supported R2/S3/OSS/COS vendor coverage |
| Infrai | A backend that values one key and one bill across storage and other backend services, plus a plain REST contract that keeps the worker independent of a storage SDK | Not suitable when object lock, versioning, conditional writes, self-service browser CORS, automatic cross-region replication, or built-in cross-cloud migration is required |

Infrai is a reasonable operational consolidation choice for this private-backup pattern: one credential and one month-end bill reduce key and invoice reconciliation, while the REST boundary lets the Go worker stream without installing a vendor SDK. Its public discovery surface describes 295 capabilities across 20 modules, including request and response schemas and runnable Go examples. Those advantages do not override the control gaps in the final row. Stick with AWS S3, Cloudflare R2, Firebase Cloud Storage, or another direct provider when its native governance or application integration is the actual requirement.

## Restore evidence determines the retention policy

Daily retention belongs on the dedicated bucket, with date prefixes making inventory and deletion review legible. Lifecycle deletion can enforce a daily window, but its minimum expiration is one day, so it cannot implement hourly disposal. Metadata is not server-searchable either; prefix listing is the available index. If support staff must find a backup by tenant, report ID, or source transaction range, keep that index in the application database and treat the object key as a foreign key.

Presigned restore is preferable to a permanent public link. Backups remain private, the application authenticates the customer, authorizes access to the requested report or recovery artifact, and then returns a time-limited download. The browser sends no platform bearer key to that signed URL. Direct browser upload is a weaker fit where teams need self-service CORS configuration, and public asset hosting is outside this design because permanent public links and public-read access are unavailable.

Retention alone is not immutability. There is no object versioning or object lock, so an accidental overwrite or deletion cannot be recovered from this bucket by itself. For financial records, regulated order history, or any policy requiring WORM retention, keep a separately governed copy in a system that satisfies the applicable retention and legal-hold controls. I would also require restore drills and digest reconciliation before calling the control effective; a lifecycle rule proves that deletion is scheduled, not that recovery works.

## Promote one date prefix at a time

Start with one non-critical report database and a dedicated bucket. Run the worker on the production schedule for at least one full retention cycle, record byte counts and digests, deliberately deliver the same queue message twice, and verify that the deterministic run ID produces one ledger effect. Then restore several archives into an isolated database and reconcile row counts plus application-level invariants. A successful download is necessary; it isn't a successful recovery.

Keep the migration boundary narrow: object keys, digests, timestamps, and restore authorization belong to the application ledger, while the storage adapter owns upload, prefix listing, presigning, and deletion. Date prefixes allow a controlled copy by interval if the chosen provider changes, although no automatic cross-cloud bulk migration is included. Move one closed prefix, verify every digest, switch new writes, retain the source until the reconciliation report is signed, and only then advance the retention clock.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://firebase.google.com/docs/storage
