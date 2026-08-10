# Crystal MQTT

[![CI](https://github.com/spider-gazelle/crystal-mqtt/actions/workflows/ci.yml/badge.svg)](https://github.com/spider-gazelle/crystal-mqtt/actions/workflows/ci.yml)

A MQTT communication library for crystal lang with pluggable transports


## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     mqtt:
       github: spider-gazelle/crystal-mqtt
   ```

2. Run `shards install`


## Usage

```crystal
require "mqtt/v3/client"

# Create a transport (TCP, Websocket etc)
tls = OpenSSL::SSL::Context::Client.new
tls.verify_mode = OpenSSL::SSL::VerifyMode::NONE
transport = MQTT::Transport::TCP.new("test.mosquitto.org", 8883, tls)

# Establish a MQTT connection
client = MQTT::V3::Client.new(transport)
client.connect

# Perform some actions
client.publish("steves/channel", "hello", qos: MQTT::QoS::BrokerReceived)
client.ping

# Subscribe to a channel
client.subscribe("$SYS/#") do |key, payload|
  # payload is a Bytes slice (to support binary payloads)
  content = String.new(payload)
  puts "#{key}: #{content}"
end

sleep 5

# Unsubscribe from a channel
client.unsubscribe("$SYS/#")

sleep 1

# Pauses the fibre here until the socket closes (disconnect)
client.wait_close

# You can also explicitly disconnect
client.disconnect

```

### Websockets

```crystal
transport = MQTT::Transport::Websocket.new("test.mosquitto.org", "/mqtt", 8081, tls)
client = MQTT::V3::Client.new(transport)
client.connect
```


## Quality of service

All three levels are supported. `publish` blocks until the handshake for the
requested level has completed.

| Level | Enum | Behaviour |
|---|---|---|
| 0 | `MQTT::QoS::FireAndForget` | returns once the packet has been written |
| 1 | `MQTT::QoS::BrokerReceived` | waits for `PUBACK` |
| 2 | `MQTT::QoS::SubscribersReceived` | waits for `PUBREC`, sends `PUBREL`, waits for `PUBCOMP` |

Inbound QoS 2 messages are held until the broker sends `PUBREL`, so a
redelivery is never dispatched to your callback twice.


## Timeouts

Every request that waits on the broker takes a `timeout`, defaulting to
`MQTT::V3::Client#timeout` (30 seconds). Set it to `nil` to wait indefinitely.

```crystal
client = MQTT::V3::Client.new(transport, timeout: 5.seconds)

client.publish("some/topic", "hello", qos: MQTT::QoS::BrokerReceived, timeout: 1.second)
```

The TCP transport also accepts socket level timeouts:

```crystal
MQTT::Transport::TCP.new("test.mosquitto.org", read_timeout: 30, write_timeout: 10)
```


## Keep alive

The client pings automatically whenever the link has been idle, at 75% of the
keep alive interval negotiated during `connect`, and closes the connection if
the broker stops responding. Pass `keep_alive_active: false` if you would rather
call `ping` yourself, or `keep_alive: 0` to disable it at the protocol level.

```crystal
client.connect(keep_alive: 30)
```


## Errors

Every error raised by this shard is a `MQTT::Error`, so a single rescue is
enough:

```crystal
begin
  client.subscribe("some/topic") { |topic, payload| handle(topic, payload) }
rescue error : MQTT::Error
  Log.error(exception: error) { "subscription failed" }
end
```

| Error | Raised when |
|---|---|
| `MQTT::TimeoutError` | the broker did not respond in time |
| `MQTT::NotConnectedError` | the transport closed, or was already closed |
| `MQTT::ConnectError` | the broker refused the connection, carries `return_code` |
| `MQTT::SubscriptionError` | the broker rejected a topic filter |
| `MQTT::ProtocolError` | the broker sent something we can't parse |
| `MQTT::PacketError` | a packet could not be encoded |


## Topic matching

`MQTT::V3::Client.topic_matches` implements the wildcard rules, including shared
subscription (`$share/group/...`) prefixes. Per the specification, `#` and `+`
at the first level do not match topics beginning with `$` — subscribe to
`$SYS/#` explicitly to receive broker system topics.


## Limits

`max_packet_size` caps how large a single packet from the broker may be,
defaulting to 8MB. A packet larger than this closes the connection rather than
being buffered.

```crystal
client = MQTT::V3::Client.new(transport, max_packet_size: 64_u32 * 1024)
```
