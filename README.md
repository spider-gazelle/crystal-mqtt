# Crystal MQTT

[![CI](https://github.com/spider-gazelle/crystal-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/spider-gazelle/crystal-mqtt/actions/workflows/ci.yml)

An MQTT client for Crystal, supporting **3.1.1 and 5.0**, with pluggable transports.


## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     mqtt:
       github: spider-gazelle/crystal-mqtt
   ```

2. Run `shards install`


## Which client

| Class | Use it when |
|---|---|
| `MQTT::Client` | you don't know what the broker speaks. Tries 5.0, falls back to 3.1.1 |
| `MQTT::V5::Client` | you want 5.0 features: properties, reason codes, topic aliases, enhanced auth |
| `MQTT::V3::Client` | you are talking to a 3.1.1 broker and want nothing more |

All three share the same transports, reconnection, keep alive and timeout behaviour.


## Usage

```crystal
require "mqtt/v5/client"

transport = MQTT::Transport::TCP.new("test.mosquitto.org", 1883)
client = MQTT::V5::Client.new(transport)
client.connect

client.subscribe("sensors/#", qos: MQTT::QoS::BrokerReceived) do |topic, payload, retained|
  # payload is a Bytes slice, so binary payloads work
  puts "#{topic}: #{String.new(payload)}#{" (retained)" if retained}"
end

client.publish("sensors/kitchen", "21.5", qos: MQTT::QoS::BrokerReceived)

client.wait_close # blocks until the connection ends
client.disconnect
```

The 3.1.1 client is identical apart from the 5.0 specific arguments:

```crystal
require "mqtt/v3/client"

client = MQTT::V3::Client.new(MQTT::Transport::TCP.new("test.mosquitto.org", 1883))
client.connect
```

### TLS and websockets

```crystal
tls = OpenSSL::SSL::Context::Client.new
MQTT::Transport::TCP.new("test.mosquitto.org", 8883, tls)
MQTT::Transport::Websocket.new("test.mosquitto.org", "/mqtt", 8081, tls)
```


## Version negotiation

`MQTT::Client` connects as 5.0 and falls back to 3.1.1 when the broker will not
take it. A broker that rejects the version **closes the connection**, so the
retry needs a fresh transport — which is why this takes a factory:

```crystal
require "mqtt/client"

client = MQTT::Client.new { MQTT::Transport::TCP.new("test.mosquitto.org", 1883) }

client.connect          # => MQTT::Version::V5
client.version          # whichever was agreed
client.v5.try &.reauthenticate  # 5.0 features via the underlying client
```

Negotiation happens once. The client that wins keeps the factory, so reconnects
go straight back at the agreed version rather than re-probing 5.0 every time.

It exposes what the two protocols have in common — `publish`, `subscribe`,
`unsubscribe`, `ping`, `disconnect`, `subscriptions`, `wait_close`. Anything
version specific stays on `#v5` or `#v3`.


## Retained messages

A retained message is held by the **broker**, not the connection, so it outlives
the client that published it and is delivered to whoever subscribes next.

```crystal
# store the current state of a device
client.publish("state/kitchen", "on", qos: MQTT::QoS::BrokerReceived, retain: true)

# a later subscriber receives it immediately, flagged as retained
client.subscribe("state/#") do |topic, payload, retained|
  if retained
    apply_stored_state(topic, payload)   # this is the value from before we connected
  else
    handle_live_update(topic, payload)   # this just happened
  end
end

# a zero length retained publish clears it
client.publish("state/kitchen", "", qos: MQTT::QoS::BrokerReceived, retain: true)
```

The third block parameter is what lets you tell stored state from a live update.
It is optional — a two parameter block still works.

5.0 adds control over whether retained messages are sent at all, and whether the
publisher's retain flag survives to you:

```crystal
client.subscribe("state/#",
  retain_handling: MQTT::V5::RetainHandling::Never,  # SendAlways, SendIfNew, Never
  retain_as_published: true
) { |topic, payload, retained| }
```


## MQTT 5.0

### Properties

Every 5.0 packet can carry properties. They are typed accessors on the packet,
and only the ones legal for that packet exist:

```crystal
client.publish("sensors/kitchen", %({"c":21.5}),
  qos: MQTT::QoS::BrokerReceived,
  content_type: "application/json",
  response_topic: "sensors/kitchen/reply",
  correlation_data: request_id,
  message_expiry_interval: 60_u32,
  payload_format_indicator: 1_u8,      # 1 == UTF-8
  user_properties: [{"tenant", "acme"}]
)
```

To read them, take the packet itself rather than topic and payload:

```crystal
handler = ->(packet : MQTT::V5::Publish) do
  packet.content_type          # => "application/json"
  packet.user_properties       # => [{"tenant", "acme"}]
  packet.correlation_data
  nil
end
client.subscribe(["sensors/#"], handler, qos: MQTT::QoS::BrokerReceived)
```

### Reason codes

Acknowledgements carry a reason in 5.0, so a broker can accept a packet and
still reject the request. A failing `PUBACK` raises rather than resolving, and a
failing `PUBREC` ends the QoS 2 exchange without sending `PUBREL`.

```crystal
begin
  client.publish("some/topic", "payload", qos: MQTT::QoS::BrokerReceived)
rescue error : MQTT::ProtocolError
  # e.g. "publish rejected: quota exceeded"
end
```

`SUBACK` reason codes are per filter, so a partial rejection names which ones
failed.

### Subscription options

```crystal
client.subscribe("sensors/#",
  qos: MQTT::QoS::BrokerReceived,
  no_local: true,              # don't echo our own publications back
  retain_as_published: true,   # keep the publisher's retain flag
  retain_handling: MQTT::V5::RetainHandling::SendIfNew,
  identifier: 42_u32           # tags delivered messages, cheaper than re-matching
) { |topic, payload| }
```

### Negotiated limits

The client honours what the CONNACK negotiated, rather than being disconnected
for exceeding it:

- **Receive Maximum** — publishes wait for an in flight slot instead of overrunning the broker
- **Maximum Packet Size** — an oversized packet raises `MQTT::PacketError` locally
- **Maximum QoS**, **Retain Available**, wildcard / shared subscription / subscription identifier availability — refused locally with a clear error

```crystal
client.server_receive_maximum        # => 20
client.server_maximum_packet_size    # => 2000000
client.server_maximum_qos            # => MQTT::QoS::SubscribersReceived
client.server_retain_available?      # => true
```

### Topic aliases

Established automatically when the broker offers them: the first publish to a
topic carries both the topic and an alias, later ones send just the alias. They
are scoped to a connection and reset when it drops.

```crystal
client.use_topic_aliases = false # to always send the full topic
```

### Enhanced authentication

The shard ships no SASL mechanism of its own — name the method the broker
expects and supply the exchange:

```crystal
client.authenticator = MQTT::V5::Authenticator.new("SCRAM-SHA-1") do |challenge|
  challenge.nil? ? initial_response : answer(challenge)
end

client.connect          # multi step challenges are answered during connect
client.reauthenticate   # re-authenticate an established connection
```

### Server initiated disconnect

A 5.0 broker can say why before closing:

```crystal
client.wait_close
client.disconnect_reason   # => MQTT::V5::ReasonCode::SessionTakenOver
client.server_reference    # => "other.broker:1883" on a redirect
```

Reason codes that mean "do not come back" (bad credentials, banned, redirected)
veto an automatic reconnect.


## Quality of service

All three levels are supported. `publish` blocks until the handshake completes.

| Level | Enum | Behaviour |
|---|---|---|
| 0 | `MQTT::QoS::FireAndForget` | returns once the packet has been written |
| 1 | `MQTT::QoS::BrokerReceived` | waits for `PUBACK` |
| 2 | `MQTT::QoS::SubscribersReceived` | waits for `PUBREC`, sends `PUBREL`, waits for `PUBCOMP` |

Inbound QoS 2 messages are held until the broker sends `PUBREL`, so a redelivery
is never dispatched to your callback twice.


## Reconnection

Pass a block that builds a transport and the client re-establishes the
connection whenever it drops, replaying the CONNECT and restoring every
subscription with its callbacks intact.

```crystal
client = MQTT::V5::Client.new(reconnect: MQTT::Reconnect.new) do
  MQTT::Transport::TCP.new("test.mosquitto.org", 1883)
end

client.connect(client_id: "my-client")
client.subscribe("sensors/#") { |topic, payload| handle(topic, payload) }
# the subscription above survives a dropped connection
```

Delays back off exponentially and are capped:

```crystal
MQTT::Reconnect.new(initial_delay: 1.second, max_delay: 30.seconds, max_attempts: nil)
```

If the broker reports `session_present` the subscriptions are already held
server side and are not sent again. When reconnection is exhausted, or you call
`disconnect`, the client is `terminated?` and `wait_close` returns.

Requests made while the connection is down fail with `MQTT::NotConnectedError` —
messages are not queued for later delivery.


## Keep alive

The client pings automatically whenever the link has been idle, at 75% of the
negotiated interval, and closes the connection if the broker stops responding. A
5.0 broker may impose its own interval, which is honoured.

```crystal
client.connect(keep_alive: 30)                        # seconds
client.connect(keep_alive: 30, keep_alive_active: false)  # ping yourself instead
```


## Timeouts

Every request that waits on the broker takes a `timeout`, defaulting to 30
seconds. Set it to `nil` to wait indefinitely.

```crystal
client = MQTT::V5::Client.new(transport, timeout: 5.seconds)
client.publish("some/topic", "hello", qos: MQTT::QoS::BrokerReceived, timeout: 1.second)

MQTT::Transport::TCP.new("test.mosquitto.org", read_timeout: 30, write_timeout: 10)
```


## Transport lifecycle

Constructing a transport does not open a socket; the client connects it once its
callbacks are in place, which is what stops data arriving before there is
anything able to process it. A connection failure therefore surfaces from
`Client.new`, not from the transport constructor.

```crystal
transport = MQTT::Transport::TCP.new("test.mosquitto.org", 1883) # no socket yet
client = MQTT::V5::Client.new(transport)                         # connects here
```


## Errors

Every error raised by this shard is an `MQTT::Error`, so a single rescue covers
them:

| Error | Raised when |
|---|---|
| `MQTT::TimeoutError` | the broker did not respond in time |
| `MQTT::NotConnectedError` | the transport closed, or was already closed |
| `MQTT::ConnectError` | the broker refused the connection, carries `return_code` |
| `MQTT::SubscriptionError` | the broker rejected a topic filter |
| `MQTT::ProtocolError` | the broker sent something invalid, or rejected a request |
| `MQTT::PacketError` | a packet could not be encoded |


## Topic matching

`MQTT.topic_matches?(filter, topic)` implements the wildcard rules, including
shared subscription (`$share/group/...`) prefixes. Per the specification, `#` and
`+` at the first level do not match topics beginning with `$` — subscribe to
`$SYS/#` explicitly for broker system topics.


## Limits

`max_packet_size` caps how large a single packet from the broker may be,
defaulting to 8MB. A larger packet closes the connection rather than being
buffered.

```crystal
MQTT::V5::Client.new(transport, max_packet_size: 64_u32 * 1024)
```


## Development

```bash
./test                        # whole suite, broker and all
./test spec/v5_live_spec.cr   # one file
```

`./test` finds a broker in order of preference: one you nominated with
`MQTT_LIVE_BROKER`, a local `mosquitto` binary, then `docker compose`. It runs
the formatter and ameba as well, and is what CI runs, so a green run locally
means a green run there.

Without a broker the suite still runs; the end to end specs report as pending
rather than silently passing.
