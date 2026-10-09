# How to Aggregate Votes and Publish 5 Second Interval Tallies with Node.js Express

**TL;DR:** Accept each resident vote as an idempotent command, commit it before acknowledging success, and let one interval worker publish a versioned tally every 5 seconds. In a property-management poll, presence helps decide which open sessions receive that tally, but it must never decide which votes count: a resident can disconnect after voting, reconnect twice, or keep several tabs open without changing the durable result. Node.js with Express can own the HTTP command boundary; the runnable Go reference below makes the concurrency and audit rules explicit without tying the design to a realtime product.

## How should Node.js aggregate votes and publish each interval tally?

A building manager may ask, "How many connected residents voted yes?" That sounds operationally convenient and is logically wrong for most ballots. WebRTC defines connectivity mechanisms for peer-to-peer media and data, while WebSocket defines a persistent two-way connection; neither connection state is a ledger of accepted business events. A transient session answers where delivery might work now. The vote table answers what the system accepted.

Keep three identities separate: `resident_id` is the eligible principal, `poll_id` selects the ballot, and `session_id` represents one temporary connection. The idempotency key belongs to the vote command, not the socket. This distinction prevents a reconnect from creating another vote and prevents a stale presence record from erasing one.

That boundary matters.

The audit constraint is equally direct. Store the submitted choice, command key, acceptance time, and an immutable sequence number in the same transaction that changes the current vote. If policy permits changing a vote, append another audit event and update the materialized choice atomically; do not edit history. Compliance requirements differ by jurisdiction and ballot purpose, so retention and access controls must come from counsel and the organization's records policy rather than a framework default.

## Step 1 commit an idempotent vote before acknowledging it

In Express, the route handler should authenticate the resident, validate poll eligibility and choice, and call a transaction with a uniqueness constraint on `(poll_id, idempotency_key)`. It returns the recorded result only after commit. A process-local map is acceptable for demonstrating the contract, but it is not a production datastore and cannot coordinate multiple Node.js processes.

This runnable Go program models the same command boundary. Repeating a command key returns the earlier result; a different key from the same resident replaces the current choice while preserving an audit event.

```go
package main

import (
    "errors"
    "fmt"
    "sync"
    "time"
)

type VoteCommand struct {
    PollID, ResidentID, Choice, IdempotencyKey string
}

type AuditEvent struct {
    Sequence uint64
    AcceptedAt time.Time
    Command VoteCommand
}

type Ledger struct {
    mu sync.Mutex
    sequence uint64
    choices map[string]string
    commands map[string]AuditEvent
    audit []AuditEvent
}

func NewLedger() *Ledger {
    return &Ledger{choices: map[string]string{}, commands: map[string]AuditEvent{}}
}

func (l *Ledger) Accept(c VoteCommand) (AuditEvent, error) {
    l.mu.Lock()
    defer l.mu.Unlock()

    commandKey := c.PollID + "/" + c.IdempotencyKey
    if prior, ok := l.commands[commandKey]; ok {
        return prior, nil
    }
    if c.PollID == "" || c.ResidentID == "" || c.IdempotencyKey == "" {
        return AuditEvent{}, errors.New("missing command identity")
    }
    if c.Choice != "yes" && c.Choice != "no" && c.Choice != "abstain" {
        return AuditEvent{}, errors.New("invalid choice")
    }

    l.sequence++
    event := AuditEvent{l.sequence, time.Now().UTC(), c}
    l.choices[c.PollID+"/"+c.ResidentID] = c.Choice
    l.commands[commandKey] = event
    l.audit = append(l.audit, event)
    return event, nil
}

func main() {
    ledger := NewLedger()
    command := VoteCommand{"boiler-2026", "unit-4b", "yes", "cmd-0187"}
    first, _ := ledger.Accept(command)
    retry, _ := ledger.Accept(command)
    fmt.Println(first.Sequence == retry.Sequence)
}
```

The mutex gives this single process a serial order. In production, put the uniqueness check, current-choice update, sequence allocation, and audit insert in one database transaction. If two requests race, the datastore constraint settles the duplicate; checking first and inserting later leaves a race window. Exactly once is the business effect created by that transaction and idempotency key, not a promise made by the network.

Retries are normal.

## Step 2 snapshot on an interval without losing updates

Do not reset counters after every publish. That tempting implementation loses a vote when acceptance races with reset, and a failed publish makes the next message incomplete. Derive each message from durable current choices instead, label it with the highest committed sequence, and allow identical snapshots to be sent again. Consumers replace an older tally only when the incoming version is greater, or harmlessly accept the same version twice.

The next program adds a 5-second ticker and emits a complete JSON snapshot. It copies state while holding the lock and performs encoding after release, keeping the critical section short.

```go
package main

import (
    "encoding/json"
    "os"
    "sync"
    "time"
)

type Snapshot struct {
    PollID string `json:"poll_id"`
    Version uint64 `json:"version"`
    GeneratedAt time.Time `json:"generated_at"`
    Counts map[string]int `json:"counts"`
}

type TallyStore struct {
    mu sync.Mutex
    version uint64
    choices map[string]string
}

func (s *TallyStore) Snapshot(pollID string) Snapshot {
    s.mu.Lock()
    version := s.version
    choices := make([]string, 0, len(s.choices))
    for _, choice := range s.choices {
        choices = append(choices, choice)
    }
    s.mu.Unlock()

    counts := map[string]int{"yes": 0, "no": 0, "abstain": 0}
    for _, choice := range choices {
        counts[choice]++
    }
    return Snapshot{pollID, version, time.Now().UTC(), counts}
}

func main() {
    store := &TallyStore{version: 3, choices: map[string]string{"unit-4b": "yes", "unit-7a": "no"}}
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    encoder := json.NewEncoder(os.Stdout)
    for range ticker.C {
        if err := encoder.Encode(store.Snapshot("boiler-2026")); err != nil {
            os.Stderr.WriteString(err.Error() + "\n")
        }
    }
}
```

Short interval, long memory. The publisher may crash between reading version 41 and sending it, so version 42 must remain a full, authoritative snapshot rather than a delta that assumes delivery of 41. If bandwidth later requires deltas, retain a replayable event log and define gap recovery; do not silently change message semantics.

This design has a real trade-off: full snapshots repeat data, a transactional outbox adds storage and cleanup work, and leader fencing adds operational state that a single-process prototype does not need. It is not appropriate for a casual poll where losing a few updates is acceptable, nor is a 5-second interval suitable when the interface requires subsecond feedback; in the latter case, publish provisional changes immediately but still reconcile them against periodic authoritative snapshots. At the other extreme, a ballot governed by formal election rules needs an independently reviewed voting system, threat model, and compliance process rather than an application tutorial. The useful middle ground is an operational property poll whose result must survive retries and reconnects, but whose display can tolerate bounded freshness.

No transport removes those costs.

## Step 3 separate publication from connection delivery

One elected publisher should generate a given poll's interval snapshot. Every application instance may deliver that snapshot to its locally connected sessions, but allowing every instance to aggregate and publish independently creates competing versions unless they share a transactional coordinator. A database advisory lock, leased job, or queue consumer group can establish single ownership; the correctness requirement is fencing, so a worker whose lease expired cannot publish after its replacement.

Use an outbox when tally creation and message publication cross a transactional boundary. The tally transaction inserts an outbox row containing `poll_id`, `version`, and payload; a relay sends it and records progress. Delivery may repeat after a crash. Consumers therefore deduplicate by `(poll_id, version)`, while the server retains enough history to reconcile a client that reports an old version.

Presence belongs after this point. Track each session with a lease and expiry, refresh it on authenticated activity or heartbeat, and route the snapshot to currently eligible sessions. Presence will occasionally lag reality because networks partition and processes stop without a clean close. Treat it as soft routing state, report its age, and never feed it back into the accepted-vote count.

| Concern | Durable authority | Failure response |
| --- | --- | --- |
| Vote acceptance | Transactional vote and audit records | Retry with the same idempotency key |
| Published tally | Versioned full snapshot plus outbox | Resend; consumers ignore older versions |
| Connected sessions | Expiring presence leases | Reconnect and refresh the lease |
| Client display | Highest applied poll version | Request a fresh snapshot after a gap |

The transport remains replaceable. WebSocket is appropriate for server-to-browser bidirectional messaging, Server-Sent Events can carry one-way browser updates over HTTP, and WebRTC data channels fit peer-to-peer communication but introduce a different connection and signaling model. Select the transport from directionality, intermediary behavior, authorization, and recovery needs. None changes the ledger rule.

## Step 4 prove recovery before rollout

Test invariants, not a smooth demo. Send the same command 100 times concurrently and require one audit sequence and one business effect. Terminate the publisher after committing an outbox row but before marking it delivered; the replacement must resend the same version. Pause a client through three ticks, reconnect it with its last applied version, and verify that a full snapshot repairs the display. Finally, expire presence while the resident's vote remains counted.

Observe accepted commands, duplicate-command hits, transaction conflicts, publication lag, outbox age, connected-session lease age, and client version gaps. Avoid resident identifiers and ballot choices in ordinary logs; connect audit access to explicit authorization and record that access separately. An alert on growing outbox age is more actionable than an alert on raw socket count, because it points to delayed business state rather than normal connection churn.

Roll out compactly: shadow-compute tallies from the vote ledger, compare them with the existing result without delivering them, then enable publication for one building and inspect version gaps and reconciliation outcomes. Expand only after duplicate commands, publisher replacement, and reconnect tests pass. The final decision rule is strict: **presence chooses recipients; the ledger chooses truth.**

## References

The protocol and runtime references below define the transport and scheduling primitives; transactional guarantees still depend on the selected datastore's documented isolation and uniqueness behavior.

## Sources

- https://www.w3.org/TR/webrtc/
- https://www.rfc-editor.org/rfc/rfc6455
- https://html.spec.whatwg.org/multipage/server-sent-events.html
- https://nodejs.org/api/timers.html
- https://expressjs.com/en/4x/api.html
