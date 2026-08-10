# Changelog

## 1.3.0

Bug fixes and robustness work across the V3 client. The public API is unchanged
except where noted under *Behavioural changes*.

### Fixed

- **Subscribing to a topic you were already subscribed to destroyed the
  subscription.** The SUBSCRIBE payload was filtered to topics whose QoS had
  increased, but the SUBACK return codes were matched against the *unfiltered*
  topic list. This sent a SUBSCRIBE with no topic filters (a protocol violation,
  [MQTT-3.8.3-3]), raised `IndexError`, and the error handler then deleted the
  callback that had just been registered — while `subscribe` still returned
  successfully. Every requested filter is now sent, and return codes are matched
  by construction.
- **`unsubscribe(topic, callback)` did not compile.** It referenced an undefined
  local. Because Crystal only type-checks instantiated methods, the failure
  surfaced in the caller's build rather than here. It also unsubscribed at the
  wrong time when a filter had several callbacks.
- **QoS 2 is now implemented.** Previously `PUBREC`, `PUBREL` and `PUBCOMP` were
  all treated as an interchangeable acknowledgement, so an outbound QoS 2
  publish resolved on the first response and never sent `PUBREL` — a conforming
  broker would retry forever. Inbound QoS 2 messages were answered with `PUBACK`
  instead of `PUBREC`. Both directions now complete the full handshake, and
  inbound messages are held until `PUBREL` so a redelivery cannot be dispatched
  twice.
- **Packet identifiers could wrap to 0**, which [MQTT-2.3.1-1] reserves. They now
  skip 0 and will not collide with a request still in flight. `PUBACK` and
  `UNSUBACK` no longer share an identifier space.
- **A remaining length above 268,435,455 was silently truncated** when encoding.
  `Header#packet_length=` now raises `MQTT::PacketError`. The decoded length is
  also invalidated correctly when the variable length fields are written.
- **Message ordering was not preserved.** Both transports spawned a fiber per
  received packet, discarding MQTT's ordering guarantee and allowing unbounded
  fiber growth. Messages are now dispatched in order by a single fiber reading
  from a bounded queue.
- **A broker rejecting a topic filter crashed the client**, because the 0x80
  failure code is not a valid `QoS`. `MQTT::SubscriptionError` is raised instead.
- Requests that failed to send leaked their entry in the pending-response
  registry; registrations are now always cleaned up.
- `connect` could not be called a second time — the resolved connection state was
  never cleared, so a subsequent call returned the stale CONNACK.
- `Transport::TCP` reported a clean shutdown for genuine IO failures, and
  `Transport::Websocket` never populated `error` at all and lost exceptions from
  `HTTP::WebSocket#run` to an unhandled fiber. `on_close` is now guaranteed to
  fire exactly once, after every already-received message has been dispatched.
- Transports began reading before the client had installed its callbacks.
  Consumption now starts explicitly, once the client is ready.

### Added

- `MQTT::V3::Client#subscriptions` returns the QoS the broker actually granted
  for each active subscription — a broker may downgrade the level you asked for.
- **Automatic keep-alive.** The client now sends `PINGREQ` when the link has been
  idle, at 75% of the negotiated interval, and closes the connection if the
  broker stops responding. Pass `keep_alive_active: false` to `connect` to manage
  pings yourself.
- **Timeouts.** `connect`, `publish`, `subscribe`, `unsubscribe` and `ping` accept
  a `timeout` and default to `MQTT::V3::Client#timeout` (30 seconds). Previously
  every request would wait forever. `Transport::TCP` accepts `read_timeout` and
  `write_timeout`.
- **A maximum packet size** (`max_packet_size`, 8MB by default). Without it a
  hostile or faulty broker could advertise a 256MB packet and force the client to
  buffer all of it.
- Every error raised by this shard is now a `MQTT::Error`. New:
  `MQTT::TimeoutError`, `MQTT::PacketError`, `MQTT::ConnectError` (carrying the
  broker's `return_code`) and `MQTT::SubscriptionError`.
- Client level test coverage, driven by an in-memory transport and a scriptable
  fake broker.

### Behavioural changes

These are deliberate corrections. They are observable, so they are called out
individually.

- **`subscribe` now raises instead of returning silently on failure.** It
  previously logged the error, removed the callbacks and returned `self`, so a
  subscription that was never established was indistinguishable from one that
  was.
- **`ping` now waits for the broker's `PINGRESP`** rather than returning as soon
  as the `PINGREQ` had been written.
- **Wildcards no longer match topics beginning with `$`** ([MQTT-4.7.2-1]).
  A subscription to `#` or `+/...` will no longer receive `$SYS` topics; subscribe
  to `$SYS/#` explicitly for those.
- **`connect(will_flag: true)` requires a `will_topic`.** A zero length will topic
  is a protocol violation ([MQTT-3.1.3-10]) and was previously sent anyway.
- Requests now fail with `MQTT::TimeoutError` after 30 seconds by default. Set
  `MQTT::V3::Client#timeout` to `nil` for the previous unbounded behaviour.

### Removed

- **The `promise` dependency is gone.** Request/response plumbing moved to
  channels: `Promise::DeferredPromise#get` spawns a fiber and allocates a channel
  per call, `Promise.timeout` costs another of each, and a promise runs its
  callbacks inline on whichever fiber resolves it — which made it unsafe to
  resolve one while holding a lock.

  No public API here ever exposed a `Promise`, so this is invisible to normal
  use. Two things to be aware of if you depended on it transitively:

  - `require "mqtt/v3/client"` no longer makes `Promise` available. Add
    `promise` to your own `shard.yml` if you use it.
  - The promise shard monkey patched `Exception#cause=` onto the standard
    library. That setter disappears with it — pass `cause` to the exception
    constructor instead.

- **The empty `MQTT::SN` module has been removed**, along with
  `MQTT::SN::DEFAULT_PORT` and `MQTT::SN::ProtocolError`. It was a placeholder
  for MQTT-SN that was never implemented.

### Internal

- `shard.yml` now declares `crystal: ">= 1.8.0"`, and CI covers that floor, a
  multi-threaded run, and lint/format.
