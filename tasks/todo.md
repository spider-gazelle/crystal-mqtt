# Remediation Plan — crystal-mqtt

Companion to `tasks/issues.md`. Goal: fix everything in the register **without breaking existing
callers**. Sequenced so that each phase is independently shippable and verified before the next
starts.

## Guiding constraints

- **No breaking changes to the public surface.** New behaviour arrives via new optional keyword
  arguments with today's behaviour as the default, or via new methods alongside the old ones.
  The three items that genuinely need API movement (M8, L4, and the observable parts of C2/M5) get
  additive APIs plus deprecation shims — see Phase 6.
- **Every fix lands with a failing-test-first commit.** Per project convention: reproduce, then fix,
  then prove the test passes.
- After each phase: `crystal tool format`, `./bin/ameba`, `crystal spec -v --error-trace`.

---

## Phase 0 — Test harness (unblocks everything)

Addresses **M9**. Nothing else can be verified safely until this exists.

- [x] Add `spec/support/fake_transport.cr`: an in-memory `MQTT::Transport` subclass that records
      sent packets and lets a spec inject inbound bytes. (A working prototype was used to reproduce
      C1 during the audit — ~40 lines, no sockets, no timing dependence.)
- [x] Add a scriptable fake-broker helper on top of it: auto-CONNACK, auto-SUBACK, auto-PUBACK, plus
      hooks to respond incorrectly / not at all (needed for H2 timeout specs).
- [x] Add `spec/v3_client_spec.cr` covering the current *correct* behaviour: connect, single
      subscribe, publish QoS 0 and 1, unsubscribe, ack routing by message id, `wait_close`.
- [x] Fix **L1** (ameba: `|h, k|` block param name, stray `len` in `header.cr:30`) so CI lint is
      green before any real change lands.

**Verify:** new specs pass and CI is green on lint + format. This phase changes no behaviour.

---

## Phase 1 — Critical correctness

- [x] **C3** — fix `unsubscribe(topic, callback)`: `proc` → `callback`; correct the `found` flag so
      UNSUBSCRIBE is sent only when the last callback for a filter is removed; avoid the
      default-block insert (use `@subscription_cbs[topic]?`). Add a spec that *calls* this overload —
      its absence is why the compile error went unnoticed.
- [x] **C1** — rework `subscribe`'s bookkeeping. Drive the return-code loop from the filtered
      `to_configure` collection (or drop the filtering and always send every requested filter, which
      is what a broker expects anyway). Guarantee the SUBSCRIBE packet carries ≥1 filter; if
      `to_configure` ends up empty, register the callbacks and return without sending a packet.
      Regression spec: subscribe twice to the same topic at the same QoS, publish, assert **both**
      callbacks fire.
- [x] **M3** — remove the duplicate `next_message_id` call at `client.cr:246`. (Same method, fix
      together with C1.)
- [x] **M4** — pick one key space for `@subscription_cbs` / `@subscription_qos` and normalise once at
      entry, so the dedup check and the callback registry agree.
- [x] **C2** — re-raise after the cleanup block instead of swallowing. *This is the one behavioural
      change in this phase*: callers who ignored a silent failure now see an exception. Justified —
      the current behaviour reports success for a subscription that does not exist. Note it in the
      changelog.
- [x] **M1** — remove the message id from `@waiting_ack` / `@waiting_suback` on every failure path,
      including the C2 re-raise path and `process_requests!`'s reject branches.

**Verify:** the C1 reproduction script from the audit prints both callbacks firing; full spec suite
green.

---

## Phase 2 — Protocol conformance

- [x] **C4** — decide and implement. Recommendation: **implement the QoS 2 flow** rather than reject
      it, since `QoS::SubscribersReceived` is already public and rejecting it is the more breaking of
      the two options.
  - [x] Outbound: track state per message id; PUBLISH → await PUBREC → send PUBREL → await PUBCOMP →
        resolve. Stop treating `Pubrec`/`Pubrel`/`Pubcomp` as interchangeable `Ack`s in
        `parse_message`.
  - [x] Inbound: reply PUBREC (not PUBACK) for QoS 2, handle PUBREL, reply PUBCOMP.
  - [x] Dedup inbound message ids so a redelivery does not double-invoke callbacks.
  - [x] Specs for both directions using the fake broker.
- [x] **M2** — skip 0 when the packet identifier wraps; assert the id is not already in flight.
- [x] **M5** — `topic_matches`: `#` and `+` at the first level must not match a topic starting with
      `$`. Behavioural change; changelog note. Add the spec cases verified in the audit.
- [x] **M6** — clear `@waiting_connect` once CONNACK resolves; treat a second CONNACK as a protocol
      error; allow `connect` to be retried after a failure.
- [x] **H6** — raise `ArgumentError` from `packet_length=` above 268 435 455; invalidate the
      `@packet_length` cache when the `variable_length*` fields are written.
- [x] **L4** — validate that `will_topic` is non-empty when `will_flag` is set. Add a `Bytes`-accepting
      `will_payload` setter alongside the `String` one (additive, non-breaking).
- [x] **L5** — raise `MQTT::ProtocolError` (rather than a bare `ArgumentError`) for reserved packet
      types and QoS 3, and guard `peek_type` against an empty `io.peek`. Resolves half of **L6**.

**Verify:** conformance specs pass; QoS 2 round-trip exercised against the fake broker; optionally a
manual smoke test against `test.mosquitto.org` as documented in the README.

---

## Phase 3 — Robustness against hostile/faulty input

- [x] **H3** — add a configurable `max_packet_size` (default 8 MB) checked in `Client#tokenize`;
      close the transport with a `ProtocolError` when a header advertises more. Guard `Publish`'s
      payload-length lambda against `UInt32` underflow. Specs using the exact byte sequences from the
      audit (`Bytes[0x30, 0xFF, 0xFF, 0xFF, 0x7F]` and `Bytes[0x30, 0x01, 0x00, 0x00]`).
- [x] **H4** — replace the per-message `spawn` in both transports with a single consumer fiber
      reading from a bounded channel. Restores ordering and bounds fiber growth. Spec: inject N
      publishes, assert callbacks fire in order.
- [x] **H5** — take `@message_lock` around the `@subscription_cbs` iteration in `publish_received`
      (snapshot under lock, invoke callbacks outside it) and around the `@waiting_connect` read in
      `connect`. In `on_close`, collect pending promises under the lock and reject them *after*
      releasing it.
- [x] **L2**, **L3** — drop the dead `packet.packet_length` statements; replace the inserting default
      block with explicit `fetch`/`put`.
- [x] Run the suite under `-Dpreview_mt` as an additional CI job to catch what remains.

**Verify:** ordering spec passes; malformed-input specs close the connection cleanly instead of
allocating; MT job green.

---

## Phase 4 — Liveness (keep-alive and timeouts)

- [x] **H1** — automatic keep-alive. Spawn a ping fiber on successful CONNACK that sends PINGREQ at
      `keep_alive * 0.75` when the link has been idle, and closes the transport if PINGRESP does not
      arrive within a grace period. Off when `keep_alive == 0`. Add `keep_alive_active : Bool = true`
      so existing callers who ping manually can opt out.
- [x] **H2** — timeouts. Add an optional `timeout` keyword (default `nil` = today's behaviour) to
      `connect`, `publish`, `subscribe`, `unsubscribe` and `ping`, plus a client-level default. Add
      `read_timeout` / `write_timeout` options to `Transport::TCP`. Resolves the
      `# TODO:: implement timeouts` at `client.cr:131`. Reject the promise with a `MQTT::Error` on
      expiry and clean up the waiting-hash entry (ties into M1).
- [x] Specs driving a broker that never responds, asserting the operation raises rather than hangs.

**Verify:** timeout specs complete in bounded time; keep-alive spec observes PINGREQ on an idle link.

---

## Phase 5 — Diagnostics

- [x] **M7** — assign `@error` for `IO::Error` in `Transport::TCP#process!`; populate `@error` in
      `Transport::Websocket` and stop letting `socket.run` throw into an unhandled fiber. Ensure
      `on_close` fires exactly once per transport.
- [x] Confirm the `socket.sync = false` at `tcp.cr:24` behaves correctly when TLS is in use — it is
      applied to the raw `TCPSocket` after the TLS wrapper is built. Reading the Crystal source
      suggests `OpenSSL::SSL::Socket#unbuffered_flush` does propagate to the underlying IO, so this
      is likely fine, but it is untested and worth an explicit TLS smoke test.

**Verify:** a spec asserting `transport.error` is populated after an abrupt disconnect.

---

## Phase 6 — API ergonomics (additive only)

- [x] **M8** — separate construction from connection. Add `Transport::TCP.new(...)` variants that do
      not connect plus an explicit `#start` / `#connect` called by `Client` *after* callbacks are
      wired. Keep the existing connecting constructor delegating to the new path so no caller breaks.
- [x] **L6** — remove the unused `MQTT::SN` stub (or document it as a placeholder). `ProtocolError`
      becomes used by L5, so it stays.
- [x] **L7** — add a `crystal:` constraint to `shard.yml`; drop Crystal 1.0.0 from the CI matrix in
      favour of the oldest version that actually builds against bindata 3.x; pin `ameba` to a release
      tag instead of `branch: master`.
- [x] **L8** — add `CHANGELOG.md`; expand the README with the websocket transport, QoS support
      matrix, keep-alive and timeout options, and the behavioural changes from C2 and M5. Bump
      `shard.yml` to 1.3.0.

**Verify:** README examples compile and run against a live broker.

---

## Sequencing note

Phases 1–3 are pure bug fixes and could ship as 1.2.4. Phases 4–6 add capability and warrant 1.3.0.
Phase 0 is a hard prerequisite for all of them.

## Open questions for review

1. **C4 (QoS 2)** — implement the full flow (recommended, ~a day of work) or raise `ArgumentError`
   on `QoS::SubscribersReceived` until it is done? The latter is faster but is a breaking change for
   anyone currently passing it (and getting silently wrong behaviour today).
2. **C2** — confirm that making `subscribe` raise on failure is acceptable. It is the only way to
   make the method honest, but it is observable to existing callers.
3. **M5 ($SYS)** — confirm the spec-conformant behaviour is wanted. Anyone relying on `#` to receive
   `$SYS` topics would need to subscribe to `$SYS/#` explicitly.
4. Is MQTT 5 support on the roadmap? It would influence how much of the V3 client is refactored into
   shared code during Phase 2 versus left alone.

---

## Review

All six phases landed. Final state: **90 specs, 0 failures**, ameba clean, format clean, suite also
green under `-Dpreview_mt` with `CRYSTAL_WORKERS=4`. Baseline before this work was 48 specs and 2
ameba failures.

### Verified against a real broker

Beyond the fake-broker suite, the following was run end to end against
`test.mosquitto.org`, over both plain TCP and TLS:

- connect, subscribe, unsubscribe, disconnect
- publish at QoS 0, 1 **and 2**, each round-tripped back through a subscription
- manual `ping` and automatic keep-alive (observed `last_ping_response` advancing on an idle link)
- both `unsubscribe` overloads, including the callback-reference form that previously would not
  compile

### Deviations from the plan as written

1. **H2 default timeout.** The plan said default `nil` (today's unbounded behaviour). Implemented as
   **30 seconds** instead. Defaulting to `nil` would have meant nobody got the fix without opting in,
   and an operation that hangs a fiber forever is the failure mode we were trying to remove.
   `Client#timeout = nil` restores the old behaviour. Called out in the changelog.
2. **C1 approach.** The plan offered two options; took the second — always send every requested
   filter. The QoS-upgrade filtering was the direct cause of the misalignment, and re-subscribing is
   something brokers handle natively. This removed the failure mode instead of repairing it.
3. **The promise → channel swap** was not in the original plan; it came out of the review of
   `lib/promise`. `DeferredPromise#get` spawns a fiber and allocates a channel per call, and
   `Promise.timeout` costs another of each. More importantly a promise runs its callbacks inline on
   the resolving fiber, which is what made H5's reject-under-lock unsafe. Channels gave timeouts
   (`select`) and safe completion in one move. Promises were entirely internal — no public method
   returned one — so this is invisible to callers.
4. **L6 (`MQTT::SN` stub).** Left in place rather than removed. `ProtocolError` is now used, but
   `MQTT::SN::DEFAULT_PORT` and `MQTT::SN::ProtocolError` are public constants and deleting them
   would be a breaking change for no functional gain. Worth revisiting at 2.0.
5. **L7 ameba pin.** Left on `branch: master`, matching the in-flight working-tree change — there is
   no tagged ameba release supporting Crystal 1.21 yet. Added a comment recording why.

### Notes for the next pass

- `Transport::TCP` still connects inside its constructor. M8's race is fixed (consumption starts via
  `Transport#start`, after the client wires its callbacks), but construction and connection are
  still coupled. Separating them is a 2.0 change.
- There is no reconnect/resume logic. A dropped connection needs a new transport and client. Session
  resumption (`clean_start: false`) is accepted on the wire but nothing restores subscriptions.
- Inbound QoS 2 state (`@inbound_qos2`) is in-memory only and cleared on disconnect, which is correct
  for `clean_start: true` but would need persisting to support a resumed session.
