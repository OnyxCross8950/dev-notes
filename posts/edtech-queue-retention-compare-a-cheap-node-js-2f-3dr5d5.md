# Edtech Queue Retention: Compare a Cheap Node.js 2FA Login SMS API

Short answer: for an edtech contact form that must verify a phone number before routing a request, start with the smallest local state machine your team can test and audit. A direct SMS send keeps the challenge lifecycle in the application and usually makes a migration easier to reason about. A dedicated OTP endpoint reduces security-sensitive glue code, but it moves part of the evidence and retention boundary elsewhere. The integration decision should be based on which boundary the team can operate, not on an advertised message price.

The bill is only one part of the design. The durable records are the real accounting: a challenge identifier, a destination reference, an expiry, an attempt count, a code digest, a send outcome, and a queue-routing decision. If those records are duplicated in a delivery service, an authentication service, and the application, the support team inherits several retention policies for one contact request. The least complex design is the one that leaves one authoritative local decision about whether a form may enter a queue.

A concrete policy might expire a challenge after 15 minutes and allow five failed attempts; those are local policy choices, not universal SMS properties. The point is to make the choice visible in code and review, rather than allowing an SDK default to become an accidental retention rule.

This matters in edtech because a single form may represent an account correction, a parent’s billing question, or an instructor’s access problem. The phone check is a bounded signal that the number was recently controlled. It is not a complete identity proof. The routing event is a separate business action and must be idempotent.

## Should a cheap Node.js 2FA login SMS API live in the auth app?

Direct send and a dedicated OTP endpoint both expose an HTTP-shaped boundary, but they put different work in the application. With direct send, the application owns random-code generation, digest storage, expiry, attempt limits, destination normalization, delivery status mapping, and verification. With an OTP endpoint, challenge generation and code comparison may be remote; the application still owns the form session, authorization to route, local correlation, and the response to an unknown result. A cheap integration does not remove those auth-app decisions; it only changes where some of them are implemented.

The comparison is more useful when it names the work rather than pretending that one interface is universally simpler:

| Approach | Access method (SDK/REST) | Getting-started cost | Suitable scenario | Main limitation |
| --- | --- | --- | --- | --- |
| Direct send with a local challenge | SDK or REST adapter behind a local interface | Higher application setup; the state machine is yours to test | A team that already has transactional storage and an outbox | The team owns replay, expiry, rate limits, and delivery ambiguity |
| Dedicated OTP endpoint | SDK or REST integration with a remote challenge lifecycle | Lower security-sensitive application code; more boundary review | A small application that wants challenge mechanics outside its handler | Retention, evidence, and availability depend partly on the remote boundary |

Neither row describes a complete authentication system. The form should bind the challenge to a local session or form identifier, and the queue should accept a command only after the local authorization transition commits. An SDK can shorten request construction; REST can make an adapter more portable. Neither changes the required state transitions.

The first implementation mistake is often treating the send response as verification. A message accepted for delivery does not establish that the recipient entered the code. The second is treating a timeout as a rejection. It is an unknown outcome, and retrying it without an idempotency key can create multiple messages or multiple local attempts.

That boundary matters. No shortcut.

## What should the contact form retain?

Retention should follow the decision that must be reconstructed. Keep a salted digest of the code rather than the plaintext value, an expiry, an attempt counter, and the identifiers needed to connect the challenge to the form. Keep the support message under its ordinary support-retention policy; do not copy the full message into delivery logs. The useful audit record says that a challenge was created, that delivery was accepted, rejected, or left unknown, that verification succeeded or failed, and which queue assignment followed.

There is a deliberate loss here. After a challenge expires, an operator should not be able to recover the submitted code from the database. That makes incident review less convenient, but it removes a secret that no longer serves the workflow. A local record can still explain the decision without preserving the value that authorized it.

The direct-send shape can remain narrow:

```go
package verification

import "time"

type Challenge struct {
	ID          string
	Destination string
	CodeDigest  []byte
	ExpiresAt   time.Time
	Attempts    int
	VerifiedAt  *time.Time
	CreatedAt   time.Time
}

type RoutingEvent struct {
	ChallengeID string
	FormID      string
	Queue       string
	Outcome     string
	RecordedAt  time.Time
}
```

The important omission is as significant as the fields shown: there is no plaintext code, no full contact message, and no provider-specific response object treated as the source of truth. The same contract can sit behind a Node.js handler, while the example keeps the state explicit and reviewable.

## Can a remote OTP boundary reduce migration risk?

It can, but only if the local contract is vendor-shaped as little as possible. A migration is easier when the queue router consumes a local result such as `verified`, `expired`, `rejected`, or `unknown`, rather than a remote status string or SDK object. The delivery mechanism can then change without rewriting support routing.

The local record should include a correlation identifier for the remote challenge, but it need not copy every remote field. Record the retention terms, deletion path, export capability, and region that the application relies on. These are compliance limits and operational facts to verify before adoption; they are not details that an authentication endpoint can make irrelevant.

The local state machine should also distinguish an unavailable remote call from a failed code. Silently treating an absent response as verified is a direct break in the security boundary. Keeping a form pending is less convenient for the user, but it is an intelligible outcome that an operator can reconcile.

```go
package verification

type State string

const (
	Pending  State = "pending"
	Verified State = "verified"
	Expired  State = "expired"
	Rejected State = "rejected"
	Unknown  State = "unknown"
)

func CanRoute(state State, formID string, alreadyRouted bool) bool {
	return state == Verified && formID != "" && !alreadyRouted
}
```

This function is intentionally unremarkable. A queue command should be derived from a committed local fact, not from whether a browser happened to receive a successful HTTP response. The exactly-once mindset applies to the state transition even though the SMS network and the HTTP client do not promise exactly-once delivery.

The queue waits.

## Which retry belongs to which record?

The contact form has at least two independent idempotency keys: one for creating or resending a challenge for a login or form session, and one for routing a particular form submission. A resend may create a new challenge version, but the previous version must be invalidated or compared by version. A queue retry must not create a second support item merely because the first enqueue response was lost.

That separation is where integration effort becomes visible. Direct send requires an outbox or equivalent way to publish a routing event after the database commit. A remote OTP flow still requires the same local outbox if routing is asynchronous; it only changes who owns challenge mechanics. In both designs, the adapter should map accepted, rejected, and unknown outcomes into a small local vocabulary.

Rate limits should cover more than the destination. Apply policy at the destination, account or session, source address, and form identifier, while accounting for the fact that a school network can put many legitimate users behind one address. The limit should be policy data with an operator-visible reason, rather than a mysterious constant embedded in a handler. Logs should contain identifiers and outcomes, not codes or full phone numbers.

A focused test matrix is more revealing than a successful integration demo:

- two identical create requests produce one active challenge;
- a correct code after expiry is rejected;
- an incorrect code increments attempts without revealing whether a destination exists;
- a lost routing response does not duplicate the support item;
- an adapter timeout remains `unknown` until an allowed reconciliation path resolves it;
- a verified form reaches the intended queue once.

The WebOTP API may help a compatible browser read a code from an SMS, but it is a client convenience. The server remains the verification authority, and the SMS content must follow the format required by that browser API and the applicable delivery path. Client autofill should never be the condition that allows a form to enter a queue.

## A decision rule for a small edtech team

Choose direct send when local storage, an outbox, and review capacity already exist, and when the team needs direct control over challenge retention and audit evidence. Choose a dedicated OTP endpoint when reducing security-sensitive application code is the priority and the team has verified what evidence remains available when the remote boundary is unavailable or a record must be examined.

The integration choice should survive a later change in delivery mechanism. Put the route decision behind a local interface, preserve the two idempotency boundaries, and retain only the records needed to explain the result. The work deliberately stopped there: no plaintext code retention, no full message in delivery logs, and no claim that SMS proves more identity than the factor can support.

The least expensive failure is the one the queue can explain.

## Further reading

- RFC 7208, Sender Policy Framework (SPF): https://datatracker.ietf.org/doc/html/rfc7208
- MDN, WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
