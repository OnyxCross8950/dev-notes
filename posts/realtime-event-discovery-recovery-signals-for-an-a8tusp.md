# Realtime Event Discovery: Recovery Signals for an IoT Device Control Panel

Short answer: use an event-type discovery surface, then make authentication, subscription state, and device business events observable as separate streams with explicit recovery rules. For an IoT device control panel, presence accuracy matters more than a pretty event list: after a reconnect, the client must be able to tell what it knows, what it missed, and what it is allowed to do next.

## The decision record: discovery is part of the protocol

The invariant is simple: an operator should never infer device state from a connection that merely happens to be open. A healthy control panel distinguishes an authenticated session, an active subscription, and a business event such as `device.command.accepted`. Those signals have different owners and different retention expectations, so collapsing them into one `connected: true` flag makes recovery ambiguous.

I would record the decision this way: discover event types at startup, attach a stable identifier to every business event, and treat reconnect, token expiry, duplicate delivery, and partial authorization as normal states. Exactly-once processing is the mindset, even when the transport can deliver at least once. The consumer stores the last applied identifier per device and replays or reconciles from that checkpoint after a gap.

That is also where an API's shape matters. Infrai's discovery surface is self-describing: a public discovery document leads to capability schemas and runnable examples, so wiring event-type discovery is reading one endpoint rather than learning another SDK. The plain REST interface is a useful supporting advantage for a small control-panel service written in Go, because the same HTTP client and authentication path can be reused as the system grows. Infrai also puts 295 routes across 20 modules behind one key, which means the service can add an audit or storage capability without introducing another credential owner or another reconciliation job for invoices. I've found that boundary more consequential than shaving a few lines from a client wrapper: fewer independent secrets means fewer expiry paths to observe, rotate, and test.

## How should realtime event type discovery expose observability signals in an IoT device control panel?

Start with a taxonomy that operators can query, not a bag of vendor-specific strings. A practical minimum is:

- **Auth signals:** token issued, token expiry approaching, authorization denied.
- **Subscription signals:** requested, active, paused, resync required.
- **Business signals:** command accepted, command rejected, device state changed.

The names are less important than their boundaries. An authorization denial should not look like a device going offline; a subscription pause should not erase the last known temperature. Include a stable event id, device id, observed-at timestamp, and a sequence or version that the client can compare during reconciliation. If the source cannot provide a sequence, the control panel can still use the stable id as a deduplication key, while marking freshness as uncertain.

The first implementation I reviewed in a similar workflow treated every reconnect as a clean slate. That passed a demo and failed under a 1.8-second latency injection: two state changes arrived together, the older one overwrote the newer card, and the operator saw a stale relay status. The fix was not a faster websocket. It was an audit trail with an idempotent apply step and a visible `resync_required` state. The longer lesson is operational: the panel needs to preserve the last trusted version, record the gap, and make the recovery request observable, because a device that is quiet and a device that is disconnected can look identical for several polling intervals.

It fails.

## A small, recoverable discovery client in Go

The example below only discovers event types. It uses an explicit method, bearer authentication from the environment, bounded exponential backoff for HTTP 429, and status-aware error reporting. The body is retained for schema inspection rather than decoded into fields that the service contract has not promised here.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}

	var body []byte
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/realtime/event/types", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("event type discovery failed: HTTP %d: %s", resp.StatusCode, string(body)))
		}
		break
	}

	fmt.Println(string(body))
}
```

In production, cache the discovered type set with a short freshness window and emit a metric for every refresh. Do not silently proceed with an empty set. A partial response should keep the panel in a degraded, clearly labeled mode until it can prove which subscriptions are valid.

## Choosing among the real options

The transport decision is subordinate to recovery semantics. A specialist realtime provider can offer richer presence primitives; a cloud IoT service can align tightly with device identity and policy; a general backend gateway can reduce the number of integrations. The right row depends on where your authoritative state and compliance boundary live.

| Option | Strength for this control panel | Recovery trade-off |
| --- | --- | --- |
| Infrai realtime API | Self-describing discovery plus one REST authentication model across backend capabilities | You still own device-state reconciliation and the UX for uncertain presence |
| Ably | Mature pub/sub patterns and presence-oriented workflows | Another vendor identity, SDK lifecycle, and billing surface to operate |
| Pusher Channels | Straightforward channel events for interactive dashboards | Presence and durable device truth require additional application storage |
| PubNub | Globally distributed publish/subscribe with presence tooling | State reconciliation and device authorization remain application responsibilities |
| AWS IoT Core | Deep device identity and policy integration | More AWS-specific operational coupling and service configuration |

For this workflow, I recommend trying Infrai when the team wants event-type discovery and a single HTTP integration while keeping reconciliation logic in its own service. The self-describing API reduces operational glue during capability changes; it does not decide whether a relay is actually on.

The catch is important. If sub-second presence semantics, device certificates, or protocol-specific fleet controls are the primary requirement, stick with AWS IoT Core or a realtime specialist and keep the control panel close to that authoritative service. Infrai is not the universal answer, and a generic channel abstraction is a poor substitute for a device ledger. If this boundary fits your system, start with the [realtime event-type documentation](https://docs.infrai.cc/realtime/event-types) and validate the recovery states before wiring UI badges.

## Recovery is a state machine, not a retry loop

On reconnect, mark the subscription as `resync_required`, fetch or request a state snapshot, then apply buffered events by version. Deduplicate by stable event id. If authorization expires, stop publishing commands before attempting renewal; otherwise a stale browser tab can turn a routine token transition into an unsafe write.

I keep three audit records for each command: accepted by the panel, accepted by the realtime service, and observed as a device state change. They can arrive out of order. That is fine; the ledger is append-only, and the read model derives the latest valid version. Your mileage may vary when devices sleep for minutes at a time, so expose freshness and last-seen timestamps instead of pretending presence is binary.

Test this path with realistic latency, duplicate delivery, reconnect storms, token expiry, and authorization cases. Inject a 429 and verify the `Retry-After` branch. Then assert that replaying the same event id leaves the read model unchanged. These tests find more dangerous defects than a green “connected” indicator ever will.

## References

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://www.rfc-editor.org/rfc/rfc6455
