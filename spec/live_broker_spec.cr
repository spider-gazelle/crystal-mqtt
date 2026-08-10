require "./spec_helper"

# Interop specs against a real broker. Skipped unless MQTT_LIVE_BROKER is set,
# so the suite still runs without one:
#
#   MQTT_LIVE_BROKER=localhost crystal spec
#
LIVE_BROKER = ENV["MQTT_LIVE_BROKER"]?
LIVE_PORT   = (ENV["MQTT_LIVE_PORT"]? || "1883").to_i

private def live_client(host : String) : MQTT::V3::Client
  MQTT::V3::Client.new(
    MQTT::Transport::TCP.new(host, LIVE_PORT, connect_timeout: 10),
    timeout: 15.seconds
  )
end

# A fresh topic per run, so repeated runs and parallel CI don't collide
private def live_topic(suffix : String) : String
  "crystal-mqtt-spec/#{Random::Secure.hex(8)}/#{suffix}"
end

{% if true %}
  describe "live broker", tags: "live" do
    if (broker_host = LIVE_BROKER).nil?
      pending "requires MQTT_LIVE_BROKER to be set" { }
    else
      # This is the property that matters for anything building state on top of
      # MQTT: a retained message is held by the broker, not the connection, so
      # it has to outlive the client that published it
      it "persists a retained message across separate connections" do
        topic = live_topic("state")

        publisher = live_client(broker_host)
        publisher.connect(client_id: "crystal-spec-pub-#{Random::Secure.hex(4)}")
        publisher.publish(topic, "stored-value", qos: MQTT::QoS::BrokerReceived, retain: true)
        publisher.disconnect

        # an entirely new connection, with no shared session
        subscriber = live_client(broker_host)
        subscriber.connect(client_id: "crystal-spec-sub-#{Random::Secure.hex(4)}")

        received = Channel(Tuple(String, Bool)).new(4)
        subscriber.subscribe(topic, qos: MQTT::QoS::BrokerReceived) do |_, payload, retained|
          received.send({String.new(payload), retained})
          nil
        end

        select
        when message = received.receive
          message[0].should eq "stored-value"
          # flagged retained, so a subscriber can tell stored state from live
          message[1].should be_true
        when timeout(15.seconds)
          fail "retained message was not delivered"
        end

        # clean up, and prove an empty retained payload clears the topic
        subscriber.publish(topic, "", qos: MQTT::QoS::BrokerReceived, retain: true)
        subscriber.disconnect

        checker = live_client(broker_host)
        checker.connect(client_id: "crystal-spec-chk-#{Random::Secure.hex(4)}")
        cleared = Channel(String).new(4)
        checker.subscribe(topic) { |_, payload| cleared.send(String.new(payload)); nil }

        select
        when leftover = cleared.receive
          fail "retained message should have been cleared, got #{leftover.inspect}"
        when timeout(3.seconds)
          # nothing arrived, which is the pass condition
        end
        checker.disconnect
      end

      it "does not flag a live publish as retained" do
        topic = live_topic("live")
        client = live_client(broker_host)
        client.connect(client_id: "crystal-spec-live-#{Random::Secure.hex(4)}")

        received = Channel(Bool).new(4)
        client.subscribe(topic, qos: MQTT::QoS::BrokerReceived) do |_, _, retained|
          received.send(retained)
          nil
        end
        client.publish(topic, "live-value", qos: MQTT::QoS::BrokerReceived)

        select
        when retained = received.receive
          retained.should be_false
        when timeout(15.seconds)
          fail "publish was not delivered"
        end
        client.disconnect
      end

      it "round trips every QoS level" do
        topic = live_topic("qos")
        client = live_client(broker_host)
        client.connect(client_id: "crystal-spec-qos-#{Random::Secure.hex(4)}")

        received = Channel(String).new(8)
        client.subscribe(topic, qos: MQTT::QoS::SubscribersReceived) do |_, payload|
          received.send(String.new(payload))
          nil
        end

        {MQTT::QoS::FireAndForget, MQTT::QoS::BrokerReceived, MQTT::QoS::SubscribersReceived}.each do |qos|
          client.publish(topic, qos.to_s, qos: qos)
          select
          when value = received.receive
            value.should eq qos.to_s
          when timeout(15.seconds)
            fail "#{qos} publish was not delivered"
          end
        end
        client.disconnect
      end
    end
  end
{% end %}
