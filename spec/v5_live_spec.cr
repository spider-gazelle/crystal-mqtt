require "./spec_helper"
require "../src/mqtt/v5/client"

# End to end specs for the 5.0 client. Skipped unless MQTT_LIVE_BROKER is set;
# `./test` starts a broker and sets it for you.
V5_BROKER = ENV["MQTT_LIVE_BROKER"]?
V5_PORT   = (ENV["MQTT_LIVE_PORT"]? || "1883").to_i

private def v5_client(host : String) : MQTT::V5::Client
  MQTT::V5::Client.new(
    MQTT::Transport::TCP.new(host, V5_PORT, connect_timeout: 10),
    timeout: 15.seconds
  )
end

private def v5_topic(suffix : String) : String
  "crystal-mqtt-v5/#{Random::Secure.hex(8)}/#{suffix}"
end

private def take(channel, label, span = 15.seconds)
  select
  when value = channel.receive
    value
  when timeout(span)
    fail "timed out waiting for #{label}"
  end
end

describe MQTT::V5::Client, tags: "live" do
  if (host = V5_BROKER).nil?
    pending "requires MQTT_LIVE_BROKER to be set" { }
  else
    it "negotiates a 5.0 connection and reports the broker's capabilities" do
      client = v5_client(host)
      ack = client.connect(client_id: "crystal-v5-#{Random::Secure.hex(4)}")

      ack.success?.should be_true
      ack.reason_code.should eq MQTT::V5::ReasonCode::Success
      # mosquitto advertises these, and absent means available either way
      client.server_maximum_qos.should eq MQTT::QoS::SubscribersReceived
      client.server_retain_available?.should be_true
      client.server_receive_maximum.should be > 0
      client.disconnect
    end

    it "lets the broker assign a client identifier" do
      client = v5_client(host)
      ack = client.connect(client_id: "")

      ack.success?.should be_true
      # MQTT-3.2.2.3.7, an empty identifier means the broker picks one
      assigned = client.assigned_client_id
      assigned.should_not be_nil
      assigned.to_s.should_not be_empty
      client.disconnect
    end

    it "round trips every QoS level" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-qos-#{Random::Secure.hex(4)}")
      topic = v5_topic("qos")

      received = Channel(String).new(8)
      client.subscribe(topic, qos: MQTT::QoS::SubscribersReceived) do |_, payload|
        received.send(String.new(payload))
        nil
      end

      {MQTT::QoS::FireAndForget, MQTT::QoS::BrokerReceived, MQTT::QoS::SubscribersReceived}.each do |qos|
        client.publish(topic, qos.to_s, qos: qos)
        take(received, "#{qos} publish").should eq qos.to_s
      end
      client.disconnect
    end

    # the property section is the whole point of 5.0, so prove it survives a
    # real broker rather than only our own codec
    it "carries publish properties end to end" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-props-#{Random::Secure.hex(4)}")
      topic = v5_topic("props")

      received = Channel(MQTT::V5::Publish).new(4)
      handler = ->(packet : MQTT::V5::Publish) { received.send(packet); nil }
      client.subscribe([topic], handler, qos: MQTT::QoS::BrokerReceived)

      client.publish(
        topic, %({"value":1}),
        qos: MQTT::QoS::BrokerReceived,
        content_type: "application/json",
        response_topic: "#{topic}/reply",
        correlation_data: Bytes[1, 2, 3],
        payload_format_indicator: 1_u8,
        user_properties: [{"tenant", "acme"}, {"trace", "abc-123"}]
      )

      packet = take(received, "publish with properties").as(MQTT::V5::Publish)
      String.new(packet.payload).should eq %({"value":1})
      packet.content_type.should eq "application/json"
      packet.response_topic.should eq "#{topic}/reply"
      packet.correlation_data.should eq Bytes[1, 2, 3]
      packet.utf8_payload?.should be_true
      packet.user_properties.should eq [{"tenant", "acme"}, {"trace", "abc-123"}]
      client.disconnect
    end

    # ---- retained messages -------------------------------------------------

    it "persists a retained message across separate connections" do
      topic = v5_topic("retained")

      publisher = v5_client(host)
      publisher.connect(client_id: "crystal-v5-pub-#{Random::Secure.hex(4)}")
      publisher.publish(topic, "stored", qos: MQTT::QoS::BrokerReceived, retain: true)
      publisher.disconnect

      subscriber = v5_client(host)
      subscriber.connect(client_id: "crystal-v5-sub-#{Random::Secure.hex(4)}")
      received = Channel(Tuple(String, Bool)).new(4)
      subscriber.subscribe(topic, qos: MQTT::QoS::BrokerReceived) do |_, payload, retained|
        received.send({String.new(payload), retained})
        nil
      end

      message = take(received, "retained message").as(Tuple(String, Bool))
      message[0].should eq "stored"
      message[1].should be_true

      subscriber.publish(topic, "", qos: MQTT::QoS::BrokerReceived, retain: true)
      subscriber.disconnect
    end

    it "clears a retained message with an empty payload" do
      topic = v5_topic("cleared")

      publisher = v5_client(host)
      publisher.connect(client_id: "crystal-v5-clr-#{Random::Secure.hex(4)}")
      publisher.publish(topic, "stored", qos: MQTT::QoS::BrokerReceived, retain: true)
      publisher.publish(topic, "", qos: MQTT::QoS::BrokerReceived, retain: true)
      publisher.disconnect

      checker = v5_client(host)
      checker.connect(client_id: "crystal-v5-chk-#{Random::Secure.hex(4)}")
      leftovers = Channel(String).new(4)
      checker.subscribe(topic) { |_, payload| leftovers.send(String.new(payload)); nil }

      select
      when value = leftovers.receive
        fail "retained message should have been cleared, got #{value.inspect}"
      when timeout(3.seconds)
        # nothing arrived, which is the pass condition
      end
      checker.disconnect
    end

    # 5.0 adds control over whether retained messages are sent at all
    it "honours retain handling of Never" do
      topic = v5_topic("retain-never")

      publisher = v5_client(host)
      publisher.connect(client_id: "crystal-v5-rh-pub-#{Random::Secure.hex(4)}")
      publisher.publish(topic, "stored", qos: MQTT::QoS::BrokerReceived, retain: true)
      publisher.disconnect

      subscriber = v5_client(host)
      subscriber.connect(client_id: "crystal-v5-rh-#{Random::Secure.hex(4)}")
      received = Channel(String).new(4)
      subscriber.subscribe(topic,
        qos: MQTT::QoS::BrokerReceived,
        retain_handling: MQTT::V5::RetainHandling::Never) do |_, payload|
        received.send(String.new(payload))
        nil
      end

      select
      when value = received.receive
        fail "retain handling Never should have suppressed the retained message, got #{value.inspect}"
      when timeout(3.seconds)
        # suppressed, as asked
      end

      # a live publish must still arrive
      subscriber.publish(topic, "live", qos: MQTT::QoS::BrokerReceived)
      take(received, "live publish").should eq "live"

      subscriber.publish(topic, "", qos: MQTT::QoS::BrokerReceived, retain: true)
      subscriber.disconnect
    end

    it "keeps the publisher's retain flag when retain as published is set" do
      topic = v5_topic("rap")
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-rap-#{Random::Secure.hex(4)}")

      received = Channel(Bool).new(4)
      client.subscribe(topic,
        qos: MQTT::QoS::BrokerReceived,
        retain_as_published: true,
        retain_handling: MQTT::V5::RetainHandling::Never) do |_, _, retained|
        received.send(retained)
        nil
      end

      # published with retain set, so with RAP the flag survives to us
      client.publish(topic, "value", qos: MQTT::QoS::BrokerReceived, retain: true)
      take(received, "retain as published").should be_true

      client.publish(topic, "", qos: MQTT::QoS::BrokerReceived, retain: true)
      client.disconnect
    end

    # ---- 5.0 subscription behaviour ----------------------------------------

    it "does not echo our own publications back when no local is set" do
      topic = v5_topic("nolocal")
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-nl-#{Random::Secure.hex(4)}")

      received = Channel(String).new(4)
      client.subscribe(topic, qos: MQTT::QoS::BrokerReceived, no_local: true) do |_, payload|
        received.send(String.new(payload))
        nil
      end

      client.publish(topic, "mine", qos: MQTT::QoS::BrokerReceived)
      select
      when value = received.receive
        fail "no local should have suppressed our own publish, got #{value.inspect}"
      when timeout(3.seconds)
        # suppressed
      end
      client.disconnect
    end

    it "tags delivered messages with the subscription identifier" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-subid-#{Random::Secure.hex(4)}")
      client.server_subscription_identifiers_available?.should be_true

      topic = v5_topic("subid")
      received = Channel(MQTT::V5::Publish).new(4)
      handler = ->(packet : MQTT::V5::Publish) { received.send(packet); nil }
      client.subscribe([topic], handler, qos: MQTT::QoS::BrokerReceived, identifier: 42_u32)

      client.publish(topic, "tagged", qos: MQTT::QoS::BrokerReceived)
      packet = take(received, "identified publish").as(MQTT::V5::Publish)
      packet.subscription_identifiers.should contain 42_u32
      client.disconnect
    end

    # a broker may answer with a failure reason code or simply close the
    # connection; either way the client must surface it rather than report
    # a subscription that does not exist
    it "surfaces an error for an invalid topic filter" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-bad-#{Random::Secure.hex(4)}")

      # MQTT-4.7.1-1, a multi level wildcard must be the last level
      expect_raises(MQTT::Error) do
        client.subscribe("bad/#/filter", qos: MQTT::QoS::BrokerReceived) { |_, _| nil }
      end
      client.subscriptions.should be_empty
    end

    it "unsubscribes and stops receiving" do
      topic = v5_topic("unsub")
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-unsub-#{Random::Secure.hex(4)}")

      received = Channel(String).new(8)
      client.subscribe(topic, qos: MQTT::QoS::BrokerReceived) { |_, payload| received.send(String.new(payload)); nil }
      client.publish(topic, "before", qos: MQTT::QoS::BrokerReceived)
      take(received, "message before unsubscribe").should eq "before"

      client.unsubscribe(topic)
      client.subscriptions.should be_empty

      client.publish(topic, "after", qos: MQTT::QoS::BrokerReceived)
      select
      when value = received.receive
        fail "should not receive after unsubscribing, got #{value.inspect}"
      when timeout(3.seconds)
      end
      client.disconnect
    end

    it "pings and records the response" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-ping-#{Random::Secure.hex(4)}", keep_alive: 60)
      client.ping
      client.last_ping_response.should_not be_nil
      client.disconnect
    end

    # ---- negotiated limits and aliases -------------------------------------

    it "reuses a topic alias after the first publish" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-alias-#{Random::Secure.hex(4)}")
      client.server_topic_alias_maximum.should be > 0

      topic = v5_topic("alias")
      received = Channel(MQTT::V5::Publish).new(8)
      handler = ->(packet : MQTT::V5::Publish) { received.send(packet); nil }
      client.subscribe([topic], handler, qos: MQTT::QoS::BrokerReceived)

      # the broker has to resolve the alias back to the full topic for us, so
      # both messages must arrive naming the same topic
      client.publish(topic, "first", qos: MQTT::QoS::BrokerReceived)
      first = take(received, "first publish").as(MQTT::V5::Publish)
      first.topic.should eq topic
      String.new(first.payload).should eq "first"

      client.publish(topic, "second", qos: MQTT::QoS::BrokerReceived)
      second = take(received, "second publish").as(MQTT::V5::Publish)
      second.topic.should eq topic
      String.new(second.payload).should eq "second"

      client.disconnect
    end

    it "sends the full topic when aliases are disabled" do
      client = v5_client(host)
      client.use_topic_aliases = false
      client.connect(client_id: "crystal-v5-noalias-#{Random::Secure.hex(4)}")

      topic = v5_topic("noalias")
      received = Channel(String).new(8)
      client.subscribe(topic, qos: MQTT::QoS::BrokerReceived) { |name, _| received.send(name); nil }

      client.publish(topic, "a", qos: MQTT::QoS::BrokerReceived)
      take(received, "first").should eq topic
      client.publish(topic, "b", qos: MQTT::QoS::BrokerReceived)
      take(received, "second").should eq topic
      client.disconnect
    end

    it "keeps publishing beyond the broker's in flight allowance" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-flow-#{Random::Secure.hex(4)}")

      topic = v5_topic("flow")
      received = Channel(String).new(128)
      client.subscribe(topic, qos: MQTT::QoS::BrokerReceived) { |_, payload| received.send(String.new(payload)); nil }

      # more messages than the broker will hold in flight at once, so the
      # flow control has to release slots as PUBACKs come back
      count = client.server_receive_maximum.to_i + 5
      count = 30 if count > 30

      count.times { |i| client.publish(topic, i.to_s, qos: MQTT::QoS::BrokerReceived) }

      seen = [] of String
      count.times { seen << take(received, "flow controlled publish").as(String) }
      seen.sort_by(&.to_i).should eq (0...count).map(&.to_s)
      client.disconnect
    end

    it "refuses a packet larger than the broker will accept" do
      client = v5_client(host)
      client.connect(client_id: "crystal-v5-size-#{Random::Secure.hex(4)}")

      limit = client.server_maximum_packet_size
      raise "broker did not advertise a maximum packet size" unless limit

      oversized = "x" * (limit + 1024)
      expect_raises(MQTT::PacketError, /maximum packet size/) do
        client.publish(v5_topic("oversized"), oversized, qos: MQTT::QoS::BrokerReceived)
      end

      # the connection must still be usable afterwards
      client.ping
      client.disconnect
    end

    it "resumes a session when clean start is false" do
      client_id = "crystal-v5-session-#{Random::Secure.hex(4)}"
      topic = v5_topic("session")

      first = v5_client(host)
      first.connect(client_id: client_id, clean_start: false, session_expiry_interval: 60_u32)
        .session_present.should be_false
      first.subscribe(topic, qos: MQTT::QoS::BrokerReceived) { |_, _| nil }
      first.disconnect

      second = v5_client(host)
      ack = second.connect(client_id: client_id, clean_start: false, session_expiry_interval: 60_u32)
      # the broker held our session, so the subscription is still there
      ack.session_present.should be_true
      second.disconnect(reason: MQTT::V5::ReasonCode::Success)
    end
  end
end
