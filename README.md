[![Build Status](https://travis-ci.org/spider-gazelle/crystal-mqtt.svg?branch=master)](https://travis-ci.org/spider-gazelle/crystal-mqtt)


# Crystal MQTT

A MQTT communication library for crystal lang


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

# Create a client
client = MQTT::V3::Client.new("test.mosquitto.org", 8883)

# Configure TLS if the transport is secure
tls = OpenSSL::SSL::Context::Client.new
tls.verify_mode = OpenSSL::SSL::VerifyMode::NONE
client.tls_context = tls

# Establish a connection
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
