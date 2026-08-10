# crystal-mqtt — Issues Register

Baseline at time of audit: `crystal spec` 48 examples / 0 failures, `crystal tool format --check` clean,
`./bin/ameba` 2 failures. Crystal 1.21.0, bindata 3.2.1, promise 3.2.1.

Status legend:
- **Verified** — reproduced by running code (command recorded in the issue).
- **Inspection** — identified by reading the code; not yet reproduced.

Breaking legend: does fixing it change observable behaviour for an existing caller?
- **No** — pure bug fix, existing correct usage unaffected.
- **Behavioural** — existing (broken) usage changes outcome; no signature change.
- **API** — signature/return type change; needs a deprecation shim to stay non-breaking.

---

## Critical

### C1 — `subscribe` to an already-subscribed topic destroys the subscription
**Files:** `src/mqtt/v3/client.cr:241-313` · **Status:** Verified · **Breaking:** No

Three compounding defects on one path. `to_configure` (line 249-263) filters out topics already
subscribed at an equal-or-greater QoS, and that filtered set becomes the SUBSCRIBE payload. But the
return-code loop (line 288-295) iterates the *unfiltered* `topics` argument while indexing
`return_codes` positionally, so the two collections are misaligned.

Reproduced with an in-memory fake transport: subscribing twice to `some/topic` at the same QoS →

1. The second SUBSCRIBE goes out with **zero topic filters** — a protocol violation
   ([MQTT-3.8.3-3]); a conforming broker must close the connection.
2. `return_codes[index]` raises `IndexError`.
3. The `rescue` at line 296 catches it and runs the cleanup block, **deleting the callback the user
   just registered** — plus `@subscription_qos` for that filter.
4. `subscribe` still returns `self`, so the caller believes it succeeded.

Net effect: the second callback never fires and the client silently degrades. Observed output:
`callbacks that fired: ["some/topic"]` where both were expected.

### C2 — `subscribe` swallows all errors and reports success
**Files:** `src/mqtt/v3/client.cr:296-310` · **Status:** Verified (same repro as C1) · **Breaking:** Behavioural

The bare `rescue error` logs, cleans up callbacks, and falls through to `return self`. A rejected
promise (socket closed, broker refused, `IndexError` from C1) is indistinguishable from success. Any
caller doing `client.subscribe(...)` has no way to detect failure. Should re-raise after cleanup.

### C3 — `unsubscribe(topic, callback)` does not compile
**Files:** `src/mqtt/v3/client.cr:336-350` · **Status:** Verified · **Breaking:** No

Line 341 references an undefined identifier `proc`; the parameter is named `callback`. Crystal only
type-checks instantiated methods, so this is latent — the moment any user calls this overload their
build fails:

```
Error: undefined local variable or method 'proc' for MQTT::V3::Client
```

The `found` flag is also wrong: it is only assigned inside `if array.empty?`, so removing one of
several callbacks for a topic leaves `found == false` and skips the (correct) decision not to send
UNSUBSCRIBE — but removing the *last* callback when `cb` was nil still reports `found = false`. And
`@subscription_cbs[topic]` (line 340) hits the hash's default block, which **inserts** an empty array
for a topic that was never subscribed.

### C4 — QoS 2 is not implemented but is accepted by the API
**Files:** `src/mqtt/v3/client.cr:213-238, 400-408, 426-444` · **Status:** Inspection · **Breaking:** No

`QoS::SubscribersReceived` is a public enum value and `publish(qos:)` accepts it, but there is no
PUBREC → PUBREL → PUBCOMP exchange.

- Outbound: `parse_message` treats `Pubrec`, `Pubrel` and `Pubcomp` as a generic `Ack` and resolves
  the waiting promise on the *first* one received. The client never sends PUBREL, so a conforming
  broker retries PUBREC indefinitely and the message is never delivered.
- Inbound: `publish_received` replies with PUBACK regardless of QoS (line 433). A QoS 2 publish must
  be answered with PUBREC.
- No dedup on `message_id`, so redelivered messages invoke callbacks twice.

Either implement the flow or reject QoS 2 explicitly at the API boundary.

---

## High

### H1 — No keep-alive; the broker will drop idle connections
**Files:** `src/mqtt/v3/client.cr:151-191, 369-377` · **Status:** Inspection · **Breaking:** No

`connect(keep_alive: 60)` promises the broker a PINGREQ at least every 60 s, but nothing in the
client schedules one. `ping` is manual-only and `last_ping_response` is exposed but never checked.
Any client that is idle longer than `keep_alive` gets disconnected by a conforming broker. There is
also no detection of a *missing* PINGRESP, which is the standard way to notice a half-open socket.

### H2 — No timeouts anywhere; every operation can hang forever
**Files:** `src/mqtt/v3/client.cr:131` (`# TODO:: implement timeouts`), `transport/tcp.cr` · **Status:** Inspection · **Breaking:** No

`promise.get` in `connect`, `publish`, `subscribe`, `perform_unsubscribe` and `ping` blocks
indefinitely if the broker never responds. The TCP transport sets neither `read_timeout` nor
`write_timeout`, so a black-holed connection hangs the calling fiber permanently — the promise is
only rejected if the socket actually closes.

### H3 — Unbounded memory from a hostile or faulty broker
**Files:** `src/mqtt/v3/client.cr:72-80`, `src/mqtt/v3/publish.cr:13-17` · **Status:** Verified · **Breaking:** No

`tokenize` returns whatever length the fixed header advertises with no cap:
`tokenize(Bytes[0x30, 0xFF, 0xFF, 0xFF, 0x7F])` → `268435460`. The tokenizer will buffer 256 MB of
attacker-controlled data before emitting a single message.

Separately, `Publish`'s payload length lambda computes `packet_length - (topic.bytesize + 2)` in
`UInt32`. A packet whose declared remaining-length is smaller than its topic field underflows to a
value near 2^32, causing a ~4 GB allocation attempt. Confirmed: `Bytes[0x30, 0x01, 0x00, 0x00]` →
`BinData::ParseError` on `Publish.payload`. It fails safe here only because the read hits EOF first.

Needs an explicit maximum packet size (configurable, default e.g. 8 MB) and a guard so the payload
length can never underflow.

### H4 — Message ordering is not preserved
**Files:** `src/mqtt/transport/tcp.cr:58-60`, `src/mqtt/transport/websocket.cr:34-38` · **Status:** Inspection · **Breaking:** No

Both transports do `spawn { @on_message.try &.call(bytes) }` per extracted frame. MQTT guarantees
ordered delivery within a QoS level; spawning a fiber per message discards that ordering and lets an
unbounded number of fibers pile up under load. `parse_message` also mutates shared client state, so
this is the root of the concurrency hazards in H5.

A single consumer fiber reading from a bounded channel preserves order and bounds memory.

### H5 — Lock discipline: unsynchronised reads and rejection-under-lock
**Files:** `src/mqtt/v3/client.cr:82-100, 163, 446-454` · **Status:** Inspection · **Breaking:** No

- `publish_received` iterates `@subscription_cbs` (line 446) **without** `@message_lock`, while
  `subscribe`/`unsubscribe` mutate it under the lock. Combined with H4 this is a genuine data race,
  and becomes a crash risk under `-Dpreview_mt`.
- `connect` reads `@waiting_connect` unlocked at line 163.
- `on_close` calls `promise.reject` for every pending promise **while holding** `@message_lock`
  (lines 91-99). Promise callbacks run inline; any callback that re-enters the client (including
  `wait_close`, which takes the same lock at line 104) hits Crystal's non-reentrant `Mutex` →
  `deadlock`/raise. Collect the promises under the lock, reject outside it.

### H6 — `Header#packet_length=` silently corrupts sizes above the varint maximum
**Files:** `src/mqtt/v3/header.cr:34-42` · **Status:** Verified · **Breaking:** No

The remaining-length field encodes at most 268 435 455. Setting anything larger wraps silently:
`packet_length = 300_000_000` encodes as `3080c6868f` and decodes back as `31564544`. Should raise
`ArgumentError`. The `@packet_length` cache (line 22-32) is also never invalidated if the
`variable_length*` fields are written directly.

---

## Medium

### M1 — Failed sends leak entries in `@waiting_ack` / `@waiting_suback`
**Files:** `src/mqtt/v3/client.cr:226-235, 269-283, 360-366`, `process_requests!` 112-145 · **Status:** Inspection · **Breaking:** No

The message id is registered in the waiting hash *before* the packet is handed to `@processor`. If
`process_requests!` rejects the promise (socket closed, IO error), the hash entry is never removed.
A long-lived client that survives transient failures accumulates dead promises indefinitely. C2's
cleanup path removes callbacks but not the waiting-hash entries either.

### M2 — Packet identifier can wrap to 0
**Files:** `src/mqtt/v3/client.cr:37-40` · **Status:** Verified · **Breaking:** No

`@message_id &+ 1` after `0xFFFF` yields `0`; verified `{0, 1}`. MQTT requires a non-zero packet
identifier ([MQTT-2.3.1-1]). There is also no check that an id is not already in flight.

### M3 — `subscribe` wastes a packet identifier
**Files:** `src/mqtt/v3/client.cr:246` and `269-280` · **Status:** Inspection · **Breaking:** No

`sub.message_id` is assigned from `next_message_id` at line 246 and then overwritten with a second
`next_message_id` at line 269. Every subscribe burns two ids and desynchronises the counter.

### M4 — Empty-topic normalisation is inconsistent
**Files:** `src/mqtt/v3/client.cr:252-262` vs `274` and `292` · **Status:** Inspection · **Breaking:** No

`key = "/" if key.empty?` normalises the key used for the `@subscription_qos` dedup check and for the
wire payload, but callbacks are registered under the raw `topic` (line 274) and the QoS is written
back under the raw `topic` (line 292). For an empty filter the two key spaces permanently diverge, so
the dedup check never matches.

### M5 — Wildcards match `$SYS` topics
**Files:** `src/mqtt/v3/client.cr:15-35` · **Status:** Verified · **Breaking:** Behavioural

[MQTT-4.7.2-1]: `#` and `+` at the first level must not match topics beginning with `$`. Verified
`topic_matches("#", "$SYS/broker")` → `true` and `topic_matches("+/x", "$SYS/x")` → `true`; both
should be `false`. The README example subscribes to `$SYS/#` explicitly, which is correct and
unaffected — but a client subscribed to `#` currently receives broker system topics it did not ask
for. Fixing this is a behaviour change, so it warrants a note in the changelog.

### M6 — `connect` cannot be called twice and never clears `@waiting_connect`
**Files:** `src/mqtt/v3/client.cr:163-165, 184-190` · **Status:** Inspection · **Breaking:** No

`@waiting_connect` is set before sending and only cleared in `on_close`. After a successful CONNACK
it still holds the resolved promise, so a second `connect` call returns the *stale* CONNACK without
contacting the broker. `parse_message` (line 386) likewise never clears it, so a second CONNACK
resolves an already-resolved promise silently instead of being flagged as a protocol error.

### M7 — TCP transport reports a clean close for IO errors
**Files:** `src/mqtt/transport/tcp.cr:63-67` · **Status:** Inspection · **Breaking:** No

`rescue IO::Error` swallows the exception without assigning `@error`, so `Client#on_close` logs
"Socket closed, stopped processing incoming messages" at debug level for a genuine transport failure.
Callers cannot distinguish a graceful disconnect from a broken pipe. The websocket transport never
sets `@error` at all, and `spawn { socket.run }` (line 14) lets an exception escape into an unhandled
fiber.

### M8 — Callbacks are registered before the transport is wired up (fully resolved in Phase 7)
**Files:** `src/mqtt/transport/tcp.cr:26`, `websocket.cr:14`, `client.cr:56-69` · **Status:** Inspection · **Breaking:** API

Both transports `spawn` their read loop inside the constructor, before `Client.new` has installed
`on_tokenize` / `on_message` / `on_close`. Today this is survivable — a nil `@on_tokenize` yields
`-1` ("need more data") so bytes are buffered rather than dropped — but it depends on that
coincidence, and a close arriving in the window is lost entirely. The transport constructor also
performs blocking DNS and connect, so there is no way to build a transport without connecting (which
is also what makes the client hard to test).

### M9 — No client-level test coverage
**Files:** `spec/` · **Status:** Verified · **Breaking:** No

The 48 passing examples cover packet encode/decode and `topic_matches` only. Nothing exercises
`Client` — connect, subscribe, publish, ack routing, close handling. C1/C2/C3 all survived because no
test ever drove the client. A ~40-line in-memory `Transport` subclass is sufficient to cover all of
it (used to verify C1 during this audit), and is a prerequisite for fixing anything above safely.

---

## Low

### L1 — Ameba failures
**Files:** `src/mqtt/v3/client.cr:49`, `src/mqtt/v3/header.cr:30` · **Status:** Verified · **Breaking:** No

`Naming/BlockParameterName` on `|h, k|`, and `Lint/UnusedExpression` on the stray `len` at
`header.cr:30`. CI runs `./bin/ameba`, so `master` is currently red on lint.

### L2 — Dead expression statements in `parse_message`
**Files:** `src/mqtt/v3/client.cr:384, 393, 402, 415` · **Status:** Inspection · **Breaking:** No

Bare `packet.packet_length` statements whose result is discarded. Presumably left over from priming
the length cache; they do nothing after the packet has been read.

### L3 — `@subscription_cbs` default block inserts on read
**Files:** `src/mqtt/v3/client.cr:49-51` · **Status:** Inspection · **Breaking:** No

`Hash.new { |h, k| h[k] = [] }` means any read creates an entry. Combined with C3's
`@subscription_cbs[topic]` this silently grows the hash with empty arrays for topics that were never
subscribed, and `publish_received` then iterates them.

### L4 — Will message fields are unvalidated and String-only
**Files:** `src/mqtt/v3/connect.cr:38-43`, `client.cr:174-179` · **Status:** Verified · **Breaking:** API

`connect(will_flag: true)` with no `will_topic` encodes a zero-length will topic
(`calculate_length` → 19), which is a protocol violation ([MQTT-3.1.3-10]). `will_payload` is typed
`String`, so binary will payloads are impossible even though `publish` payloads support `Bytes`.

### L5 — Reserved / invalid enum values crash the parser
**Files:** `src/mqtt.cr:76-78`, `src/mqtt/v3/header.cr:10-15` · **Status:** Verified · **Breaking:** No

`MQTT.peek_type` on a reserved packet type raises `ArgumentError: Unknown enum
MQTT::RequestType value: 0`, and QoS bits of `3` raise `Unknown enum MQTT::QoS value: 3`.
`parse_message`'s blanket `rescue` turns both into a transport close, which is the right *outcome*
but arrives as an opaque "failed to parse message" rather than a `ProtocolError`. `peek_type` also
assumes `io.peek` returns at least one byte.

### L6 — `ProtocolError` and the `MQTT::SN` module are unused
**Files:** `src/mqtt.cr:88-108` · **Status:** Inspection · **Breaking:** No

`ProtocolError` is defined but never raised anywhere. `module SN` contains only a port constant and
another unused error class — a stub for MQTT-SN that was never implemented. Either wire
`ProtocolError` into the parse paths (see L5) or drop the dead declarations.

### L7 — CI matrix pins Crystal 1.0.0
**Files:** `.github/workflows/ci.yml:15` · **Status:** Inspection · **Breaking:** No

The matrix still builds against Crystal 1.0.0 (released 2021) while the lockfile has moved to
bindata 3.2.1 / promise 3.2.1. `shard.yml` declares no `crystal:` version constraint at all, so
`shards` cannot warn consumers. The `ameba` dependency is pinned to `branch: master`, which makes CI
non-reproducible.

### L8 — Documentation gaps
**Files:** `README.md`, repo root · **Status:** Inspection · **Breaking:** No

No CHANGELOG. The README does not mention that QoS 2 is unimplemented (C4), that keep-alive is not
automatic (H1), or that there are no timeouts (H2) — all things a user finds out in production. The
`Transport::Websocket` transport is undocumented.

---

## Resolution

All 27 issues are resolved as of v1.3.0 — see `tasks/todo.md` for the phase breakdown and
`CHANGELOG.md` for the user-facing summary. Two were closed by decision rather than code:

- **L6** — `ProtocolError` is now raised by the parse paths, and the dead `MQTT::SN` stub has been
  removed along with the `promise` dependency (see the CHANGELOG for the two transitive effects).
- **L7** — the ameba dependency stays on `branch: master`; no tagged release supports Crystal 1.21
  yet. CI now builds the ameba target explicitly (`shards install` does not build targets) and tests
  Crystal latest and nightly. No `crystal:` constraint is declared — pinning a floor we do not test
  is a claim rather than a guarantee.

Every issue marked *Inspection* above now has a spec covering it, so the register is reproducible
rather than asserted.

## Summary

| Severity | Count | IDs |
|---|---|---|
| Critical | 4 | C1–C4 |
| High | 6 | H1–H6 |
| Medium | 9 | M1–M9 |
| Low | 8 | L1–L8 |

Only three items require any API change to fix: M8 (transport construction), L4 (will payload type),
and C2/M5 in the sense that correct-but-different behaviour becomes observable. Everything else is a
straight bug fix behind the existing public surface.
