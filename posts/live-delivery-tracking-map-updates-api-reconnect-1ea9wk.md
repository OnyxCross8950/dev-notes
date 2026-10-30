# Live Delivery Tracking Map Updates API — Reconnect Design for Many Viewers

**Short answer:** for live delivery tracking map updates, use one ordered API stream with a compact snapshot, a bounded replay window, and an idempotent cursor; fall back to a snapshot when a reconnect is older than that window.

The dominant cost in a live delivery map is usually retention, not the act of moving a marker. Keeping every location sample available for every viewer turns a short-lived observation stream into an accidental archive. The practical design is to retain a compact current snapshot, keep a bounded replay window, and make a reconnect ask for a fresh snapshot whenever its cursor is older than that window.

The first design temptation is to treat every redraw as history. That is the wrong unit.

Consider the support poll as the smaller model of the same problem. Its audience may watch a changing tally while an agent moderates the session. If the service stores every browser redraw, the retained data describes rendering behavior rather than votes. If it stores only the final tally, a reconnect cannot explain what the viewer missed. The useful middle is an accepted vote event, a current tally snapshot, and a short sequence of tally changes. For a delivery map, substitute accepted coordinate observations and a current driver position. The viewer sees a coherent projection; the audit record preserves the business fact. This is why I separate the three tiers before selecting a transport or sizing a replay buffer: fan-out changes delivery work, while retention changes the long-lived data shape.

That rule also fits a customer-support session running a live poll: viewers need the current tally and a defensible sequence of changes, not an infinite transcript of every redraw. I would keep the poll's answer state and the courier's latest coordinate as separate records, then give both the same explicit cursor semantics. The map can recover quickly; the audit trail can remain exact.

## What is the bill actually buying?

There are three different things in the system, and confusing them creates the bill. A current state is one compact value per tracked subject. A replay window is a bounded sequence of changes used to repair a brief disconnect. An audit record is the durable evidence needed to explain what the service accepted and when. Only the second grows with viewer-facing fan-out, and only the third should be treated as long-lived evidence.

Three retention tiers. One cursor rule.

The redraw is not evidence.

For a delivery map, storing one latest coordinate per driver is cheap to reason about. Storing each coordinate for each viewer is not the same product: it multiplies transient presentation state by audience size. The change that moves the dominant term is to publish one ordered stream per operational scope and let viewers maintain local projections. A viewer that reconnects with a usable cursor receives changes; one that has fallen outside the replay window receives a snapshot followed by changes newer than that snapshot.

This is also the right boundary for a support poll. A vote changes the poll state once; ten browser redraws do not create ten business events. The poll aggregate, the map snapshot, and the replay buffer have different retention requirements. I would deliberately stop keeping old redraws and expired transient snapshots. When something goes wrong, that choice costs a little forensic convenience, which is why accepted business events belong in an audit store with an idempotency key, while rendering events do not.

## How should a live delivery tracking map API handle updates?

A reconnect must recover an ordered view, not merely reopen a socket. The client sends the last cursor it applied. The server checks whether that cursor is still covered by the bounded log. If it is, the server sends the missing events in order. If it is not, the server sends a new snapshot and identifies the snapshot cursor, after which the client applies later events. The API can expose this as one resume operation rather than a catalogue of transport-specific routes, because the meaningful contract is the cursor outcome: replay or resynchronize.

The cursor needs an ordering domain. A timestamp alone is a poor authority because clocks can move, samples can share a timestamp, and two partitions can produce ambiguous order. A monotonically increasing sequence scoped to the poll or tracking session is easier to validate. It does not claim that the whole world is globally ordered; it only gives one client a precise point from which to continue.

Here is the part I would keep boring and explicit. A duplicate delivery is harmless only if applying an event is idempotent. The consumer records the highest applied sequence for its scope, rejects an older sequence, and treats a repeated event identifier as already processed. Exactly-once delivery is not something a network connection can promise, so the application should provide exactly-once effects at its state boundary.

```go
package replay

import "errors"

var ErrSnapshotRequired = errors.New("cursor is outside replay window")

type Event struct {
	ID      string
	Seq     uint64
	Payload []byte
}

type Cursor struct {
	Scope string
	Seq   uint64
}

type Store interface {
	ReadAfter(scope string, seq uint64) ([]Event, error)
	Snapshot(scope string) (Cursor, []byte, error)
}

// Resume returns either a bounded replay or a snapshot authority.
func Resume(store Store, cursor Cursor) (Cursor, []Event, []byte, error) {
	events, err := store.ReadAfter(cursor.Scope, cursor.Seq)
	if errors.Is(err, ErrSnapshotRequired) {
		base, state, snapshotErr := store.Snapshot(cursor.Scope)
		return base, nil, state, snapshotErr
	}
	return cursor, events, nil, err
}
```

The snapshot and its cursor must be produced as one logical read. Otherwise a location can move between the two operations, leaving a viewer with a state that cannot be reconciled with the following event. The same concern applies to a poll tally: the snapshot is useful only if its sequence says exactly which accepted vote changes it includes.

## How should the map and poll share a contract?

They should share protocol rules, not business schemas. A courier update might contain a driver identifier, latitude, longitude, and observation time. A poll change might contain an option identifier and a count. Both need a scope, sequence, event identifier, snapshot cursor, and an explicit indication that the client must resynchronize.

The API should also distinguish accepted input from observed state. A driver device can retry a location submission after a mobile handoff; the ingestion endpoint needs an idempotency key, validation, and an audit entry. The viewer stream should expose the resulting state transition, not every retry. For a poll, the vote receipt and the tally transition follow the same pattern. This separation prevents a noisy connection from becoming a business ledger.

The limitation is deliberate: a bounded replay window cannot repair an arbitrarily long outage. A viewer that returns after the window must accept a snapshot, and a service that requires complete historical reconstruction must read the audit store instead. That trade-off keeps the map responsive without pretending that a presentation stream is a ledger.

WebSockets are a reasonable delivery transport when a browser needs server-initiated updates, but they do not provide durable replay or business ordering by themselves. WebRTC data channels can be useful for peer-oriented media or data paths, yet the same cursor, snapshot, and audit decisions still belong to the application. The transport is replaceable; the recovery contract is not.

I would test this contract with a small failure matrix: disconnect before an event, disconnect after receipt but before acknowledgement, duplicate delivery, a cursor at the replay boundary, and a cursor older than the window. Add an out-of-order input test and a snapshot taken during a state transition. These tests catch more than a happy-path browser demo because they exercise the point where “live” becomes “trustworthy.”

## What should be retained after the session ends?

Retain accepted domain events and the audit metadata needed to reconstruct their effect. Retain transient replay data only for the operational recovery period chosen by the service's requirements. Discard presentation redraws and expired snapshots. This division limits storage growth without making reconciliation impossible.

The operational signals should follow the same distinctions: lag between accepted input and published state, replay hits, snapshot fallbacks, duplicate applications, rejected cursors, and audit-write failures. A rising snapshot-fallback rate means the retention window or client behavior deserves attention; it does not automatically mean the transport is failing. A rising duplicate-application count points toward retries or consumer restarts and should be investigated without counting those duplicates as new votes or new driver positions.

The decision rule is short. If viewers need only the latest position, publish snapshots. If they need continuity across ordinary reconnects, add a bounded ordered replay. If the business must explain what happened, add durable idempotent domain events and an audit trail. Do not ask the map stream to serve all three jobs.

## Further reading

- W3C, “WebRTC 1.0: Real-Time Communication Between Browsers”: https://www.w3.org/TR/webrtc/
- IETF, RFC 6455, “The WebSocket Protocol”: https://www.rfc-editor.org/rfc/rfc6455
- MDN, “The WebSocket API”: https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
