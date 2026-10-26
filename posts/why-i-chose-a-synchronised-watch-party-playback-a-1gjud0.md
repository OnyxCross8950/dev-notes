# Why I Chose a Synchronised Watch Party Playback API Approach: Host Drift

**Short answer:** use a host-authoritative playback API with epoch fencing, small drift corrections, and three retention tiers: live state, a bounded recovery window, and compact audit decisions.

A synchronised watch party does not need a perfect copy of every playback event. It needs an authoritative host clock, a bounded estimate of each participant's presence, and enough history to repair a reconnect without turning the service into a video archive. My choice is a three-tier retention model: keep the current session state hot, keep a short event window for recovery, and retain only compact audit facts after that window expires. The trade-off is deliberate. A late participant may lose the ability to reconstruct every intermediate pause, but the system still answers the operational question that matters: who was present at a given poll moment, and which playback epoch was authoritative?

For an e-commerce watch party, presence accuracy is usually the harder requirement than sub-millisecond playback symmetry. A live poll might open while the host is at 00:42.500, while one viewer is buffering and another tab is hidden. Treating all three clients as equivalent produces a tidy stream and a bad result. I model the host's position as a signed-in-session fact, then let clients correct toward it in small steps. This also gives reconciliation a finite, inspectable record instead of a permanent event log.

## What is the bill actually made of?

The dominant storage term is not the room row. It is the repeated telemetry emitted by every participant. Suppose 2,000 viewers report position every 250 milliseconds: that is 8,000 reports per second before reconnects, retries, or metadata are counted. The arithmetic is simple, but the retention consequence is easy to miss: a presence system can create more durable data from observations than from the business event it exists to support.

I would therefore separate three lifetimes. The live session map contains the latest observed state and expires with the room. A bounded recovery window contains host transitions, poll boundaries, and selected participant checkpoints. The durable audit record keeps facts needed to explain an eligibility decision: session identifier, playback epoch, participant identifier, observation time, and decision outcome. It does not keep every position sample.

That is the cost decision. We stop keeping high-frequency samples once they have ceased to be useful for recovery. The failure cost is real: after the window closes, an investigator cannot replay every tiny oscillation in a client's clock. They can still determine which host epoch was active, whether a participant had a fresh presence observation, and why a poll decision was accepted or rejected. For payment and ledger work, this distinction is familiar: an audit trail should explain a state transition, not pretend that every transport packet is a financial fact. The 15-second observation bound in the example is a policy boundary, not a claim about every network.

That boundary is intentional.

Keep it finite.

Consider the poll that opens at 00:42.500. One participant sends a fresh report at 00:42.300, loses the connection, and returns after the recovery window has expired. The durable record should not claim that this participant watched continuously; it should record the last accepted observation and the epoch at the poll boundary, then make the result depend on the documented freshness rule. If the participant returns before expiry, the server can use the current snapshot and bounded transition history to establish a new baseline. If the return is later, the absence of intermediate samples is a known limit of the evidence, not a reason to invent continuity. That distinction keeps the poll result explainable when a support case arrives months after the live session.

## How should a synchronised watch party playback API handle drift?

A client should not infer presence from playback position alone. A paused browser can display a plausible position; a background tab can stop timers; a reconnect can deliver an old message after a newer host transition. Presence needs a timestamped heartbeat, a client sequence number, and the host epoch that produced the playback state. Drift correction then becomes a guarded state transition rather than a visual guess.

The host publishes snapshots such as `{epoch, positionMs, playing, emittedAt}`. A participant estimates the expected position using elapsed time only when `playing` is true. If the local error is small, rate adjustment is less disruptive than a seek. If the error is large, a seek is justified. Neither operation should advance the participant's presence lease: only a fresh, authenticated observation does that.

Here is the server-side shape I use for accepting a report. The important detail is the epoch check. A report from the previous host state is stale even when its wall-clock timestamp looks recent, and accepting it would make poll eligibility depend on message arrival order.

```go
package party

import "time"

type Report struct {
	ParticipantID string
	Epoch         uint64
	Sequence      uint64
	PositionMS    int64
	ObservedAt    time.Time
}

type Presence struct {
	Epoch      uint64
	Sequence   uint64
	PositionMS int64
	ObservedAt time.Time
}

type Session struct {
	Epoch      uint64
	Latest     map[string]Presence
	PollOpened time.Time
}

func AcceptReport(s *Session, r Report, now time.Time) bool {
	if r.Epoch != s.Epoch || r.ParticipantID == "" {
		return false
	}
	if r.ObservedAt.After(now.Add(2 * time.Second)) ||
		r.ObservedAt.Before(now.Add(-15 * time.Second)) {
		return false
	}
	previous, ok := s.Latest[r.ParticipantID]
	if ok && r.Sequence <= previous.Sequence {
		return false
	}
	s.Latest[r.ParticipantID] = Presence{
		Epoch: r.Epoch, Sequence: r.Sequence,
		PositionMS: r.PositionMS, ObservedAt: r.ObservedAt,
	}
	return true
}
```

The numeric bounds are policy inputs, not universal truths. I would measure clock skew, network delay, and the poll's eligibility window before setting them. A single fixed tolerance across every session is attractive because it is easy to document, but it silently changes the meaning of presence when network conditions change.

## What should survive a reconnect?

The recovery record should contain enough information to establish a new baseline, not enough to reconstruct a movie from telemetry. On reconnect, the participant receives the current epoch and snapshot, then sends one new report with a higher sequence number. The server can replay the bounded host-transition window when the client needs context; it does not need to replay all participant samples.

This is where idempotency matters. A reconnect request may be retried, and a delivery may be duplicated. Assign each accepted state-changing operation an idempotency key, persist the result for the key's useful lifetime, and make duplicate submissions return the original decision. Exactly-once delivery is not a property to assume from a realtime transport. It is an application-level outcome built from monotonic sequences, epoch fencing, and durable decisions.

WebRTC can carry peer media and data, while WebSocket is a common choice for server-mediated control messages; neither standard by itself defines the business meaning of presence, poll eligibility, or retention. The W3C WebRTC specification describes browser APIs and transport behavior, but the session contract remains yours. That boundary should be explicit in tests: transport connectivity is one assertion, authoritative epoch handling is another, and the poll decision is a third.

## Which tests catch the expensive mistakes?

I would test the state machine with a small adversarial matrix rather than rely on a happy-path playback demo. Deliver a report twice. Deliver an older sequence after a newer one. Change the host epoch while an old report is in flight. Advance time past the presence lease. Reconnect with a duplicate idempotency key. Open the poll at the same logical instant as a host transition. Each case should produce a stable decision and an audit entry that names the accepted epoch.

Operationally, watch rejection reasons, not just connection counts. A rising stale-epoch rate can mean normal host replacement, but it can also indicate delayed delivery or a client that never applied a snapshot. Track the age distribution of accepted observations, the recovery-window hit rate, duplicate-key rate, and the number of poll decisions requiring manual review. These measurements tell the team whether the retention policy is useful; raw message volume mostly tells you that clients are talking.

Compliance limits belong in the design before retention is implemented. Participant identifiers, timestamps, and eligibility outcomes may be personal data in the relevant jurisdiction, so access controls, deletion rules, and documented retention purposes need to match the deployment's legal requirements. A short recovery window does not erase the need for an audit policy. It only limits what the realtime subsystem keeps by default.

## The decision rule

Choose the smallest retention window that can repair the reconnects your product promises, then preserve compact decisions for the period required by audit and compliance. Keep the host epoch authoritative, reject stale reports, and make drift correction subordinate to fresh presence evidence.

The result is less convenient than retaining every sample, but it is easier to reconcile and harder to misinterpret. A viewer can be visibly close to the host and still have an expired presence lease. That is the edge case the design must represent.

## Further reading

- https://www.w3.org/TR/webrtc/
- https://www.rfc-editor.org/rfc/rfc6455
- https://developer.mozilla.org/en-US/docs/Web/API/Page_Visibility_API
- https://www.rfc-editor.org/rfc/rfc9110
