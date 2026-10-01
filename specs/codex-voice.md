# Native Codex voice lifecycle

This experimental V2 contract lets a native Codex voice client consume the same
OpenCode Session as typed prompts, tools, permission approvals, and questions.
No voice-specific agent loop or provider execution path is introduced.

## Capability check

Before admitting any prompt, require `GET /api/health` to return
`{healthy: true, sessionWorkProtocolVersion: 1}`. Work-scoped controls additionally
require `sessionWorkControlProtocolVersion: 1`. Older V2 servers accept prompts
but cannot guarantee this work lifecycle; do not infer support from version
strings, generic V2 route availability, or idle status.

## Admission and work

`POST /api/session/:sessionID/prompt` returns one durable `SessionInput.Admitted`.
Clients should supply a stable message ID, retain the response, and retry that
same ID only with identical Session, prompt, and delivery. `resume: false` admits
without executing; retrying an identical admission with `resume: true` requests
an advisory wake. An admission is not a completed provider turn.

- `session.next.work.started` carries an opaque `workID`, `sessionID`, timestamp,
  and initially empty `inputMessageIDs`.
- Each durable `session.next.prompted` adds its exact `messageID` to `workID`
  when the serialized runner promotes the input into visible history.
- `session.next.step.started` and `session.next.text.started/delta/ended` carry
  `workID` and the exact `inputMessageIDs` known at that provider turn. Historical
  events without these optional fields remain readable.
- `session.next.work.settled` carries the same work ID, exact promoted input IDs,
  and `outcome: completed | failed | cancelled`. Failures have a public generic
  `error: {type: unknown, message}`; credentials and raw defects are not copied.
- If initialization fails before selected inputs can promote, settlement includes
  `pendingInputMessageIDs`. Those admissions remain pending. Report the blocked
  attempt and require an explicit retry; do not mark these inputs completed.

A work spans provider steps, local tool settlement, and steering admitted at safe
boundaries. A queued input starts a new work after the current continuation
settles. Earlier queued admissions can remain pending while later steers run;
there is deliberately no cumulative admission-sequence completion watermark.
Each work accepts at most 1024 exact input IDs. Additional inputs remain pending
for a later work, without truncation or loss. Provider/tool continuation for the
already promoted inputs can continue after this limit is reached.

A work is not a process-local drain. Drains can coalesce wakes and contain several
works. Provider step finish, item finish, and process idle are not whole-work
settlement. Recoverable local tool failures may become model-facing tool results
and continue. Permission decline, question dismissal, and explicit interruption
settle the active work as cancelled. No provider call is inferred safe to retry
merely because a connection or process disappeared.

## Work-scoped typed controls

A client-local turn map can lag SSE delivery. Never implement a turn-scoped steer
or interrupt by checking that map and then calling an unconditional Session API.
Use the dedicated guarded endpoints; an older server does not recognize them:

- `POST /api/session/:sessionID/work/:workID/prompt` with `{id, prompt}`
- `POST /api/session/:sessionID/work/:workID/interrupt` with no body

Require the health capability `sessionWorkControlProtocolVersion: 1`, and fail
closed until the authoritative work ID is known. The ordinary prompt and Session
interrupt endpoints retain their existing behavior for non-work-scoped callers.

Guarded steering waits until that work's next safe boundary, which may be delayed
by a provider, tool, approval, or question. At the boundary, one durable
`session.next.prompted` transaction atomically admits and promotes the input,
records its exact `workID`, and inserts the inbox row. Its returned `Admitted`
has `admittedSeq === promotedSeq`; this path does not emit a separate
`session.next.prompt.admitted` event. Consumers must accept Prompted as evidence
of this atomic admission. No partially admitted row can escape to a later work.

Each work allows at most 64 distinct pending guarded steers and 1024 total
promoted inputs. Repeated requests with the same message ID and encoded prompt
join one pending result, including when the queue is full. Conflicting payloads
fail without altering the original request. Duplicate waiters share the original
30-second queue deadline; one waiter's cancellation cannot withdraw another
waiter's live request. Deadline expiry rejects the whole logical request before
commit. Cancelled/rejected request identities are retained until work ends, with
a bound of 1024 retained identities; a delayed retry cannot revive a rejected
request. Overflow fails closed. Successfully committed retries reconcile from
the durable inbox row and its promotion event, including after work settlement
or while another work is running.

A stale, closing, expired, or capacity-exhausted operation returns HTTP 409 without
making a new admission or interrupting a successor. A conflicting message ID may
already identify a different existing admission and must not be silently reused.
Work-scoped interrupt atomically captures and signals only that work's owner,
rejects its still-unadmitted guarded steers, and waits for work cleanup. Its
success does not grant permission to cancel a later queued work. A commit already
in progress completes atomically and remains a member of the interrupted work;
its exact-ID retry is authoritative if the response is lost.

### Known Bun 1.3.14 disconnect limitation

An HTTP timeout or TCP disconnect is an uncertain response, not cancellation.
Bun 1.3.14's `node:http` server can fail to emit response/socket close for an
aborted pending response, even when an external Node fetch client aborts and
exits. The server may therefore retain the request until the work's next safe
boundary or the server's own 30-second queue deadline. Do not claim that a local
AbortController, a missing response, or voice shutdown withdrew the request.
Retain its exact message/work IDs and reconcile by exact retry and authoritative
work membership. Use the explicit work-scoped interrupt when cancellation is
intended; the real HTTP regression verifies this route after a lost response.
Observed in-process cancellation still removes a queued request when it has no
other waiter. Once commit begins, its outcome is reconciled rather than rolled
back or reported as a false no-admission conflict.

## Assistant items and phase

OpenAI Responses message items carry their provider item ID and the explicitly
supplied `commentary` or `final_answer` phase in `providerMetadata.openai`.
Item-added starts the text item; text deltas preserve the metadata; item-done
ends it with the provider's authoritative complete text. Duplicate item-done
frames do not open or end the item twice. An item-done received without its added
frame still produces a complete start/end lifecycle.

Null, missing, and unknown phases remain unlabelled. A finish reason, empty
stream, drain idle, or absence of tool calls never fabricates `final_answer`.
Cancellation/failure flushes retain known metadata for any partial text.
Projected `AssistantText` retains provider metadata and provider text ID;
projected assistant messages retain work correlation. Subsequent requests to the
same model preserve phase and item boundaries. Provider metadata is not reused
across model changes or from a failed assistant message.

## Approval correlation

Each permission request receives a fresh core-assigned `generation` nonce, so even
identical payloads with reused caller IDs represent distinct approval instances.
Permission reply accepts optional `expectedRequest`, the complete Request shown
to the user. Native voice clients must submit it with their displayed-request
fingerprint. The core compares encoded schema values structurally under the reply
serialization guard before accepting a reply. Changed/reused IDs return HTTP 409
without settling the replacement or saving a wider permission. Missing requests
return 404. Route-level ownership checks also pass their read snapshot to core.
Question requests are generated internally with a fresh ID; their API does not
permit caller-supplied request IDs. Existing approval and question UI remains
compatible; voice never treats a tool request as implicit user approval.

## Streaming, replay, and reconnect

- `/api/event` is the live native SSE feed, including transient text deltas.
- `/api/session/:sessionID/history?after=N` provides finite durable history pages.
- `/api/session/:sessionID/event?after=N` replays durable events strictly after
  aggregate sequence N, then follows durable events.

Persisted `text.started` and `text.ended` carry identity and the ended full text;
text deltas are intentionally live-only. On reconnect, use the durable full
value to reconcile an item, rather than appending it to previously seen deltas.
Deduplicate durable events by event ID / Session aggregate sequence and items by
Session + assistant message ID + text ID. Provider-local IDs may repeat in later
provider turns. Joining a Session's current activity does not change its identity.

A started work with no settlement after a crash remains unresolved. Explicit
resume creates a new work and must not close an orphaned prior work or claim its
inputs completed. This change does not implement multi-node ownership or an
automatic post-crash provider retry policy. The existing process-global Session
coordinator remains the execution owner.

## Verification

Use Bun 1.3.14 and the locked workspace dependencies. Run tests from package
folders, and use each package's `bun typecheck` script. Public schema changes
require `bun run generate` from `packages/client`; generated sources are never
edited directly.

Coverage includes provider phase and item lifecycle, authoritative ended text,
unlabelled output, duplicate ends, historical projection replay, exact
queue/steer membership, multi-step tool work, cancellation, orphaned work,
pre-promotion blocking, membership bounds, exact admission retries, and actual
HTTP/SSE reconnect replay in
`packages/opencode/test/server/httpapi-voice-lifecycle.test.ts`.
