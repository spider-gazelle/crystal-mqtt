require "./spec_helper"

module MQTT::V3
  # Reconnection is driven by an exponential backoff, so keep the delays tiny
  FAST_RECONNECT = MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 20.milliseconds)

  describe "reconnection" do
    it "re-establishes the connection when it drops" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)
      client.connect(client_id: "steve")

      broker.transports.size.should eq 1
      broker.drop!

      # NOTE:: wait on the CONNECT itself. A new transport exists before the
      # replayed CONNECT has been written, so counting transports races
      eventually { broker.all_sent_packets(Connect, MQTT::RequestType::Connect).size == 2 }
      broker.transports.size.should eq 2
      client.closed?.should be_false
      client.terminated?.should be_false

      # the replayed CONNECT must carry the original options
      connects = broker.all_sent_packets(Connect, MQTT::RequestType::Connect)
      connects.each(&.client_id.should(eq "steve"))

      client.disconnect
    end

    it "restores subscriptions and keeps delivering to the same callbacks" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)
      client.connect

      received = [] of String
      client.subscribe("sensors/#", qos: QoS::BrokerReceived) { |topic, _| received << topic; nil }

      broker.publish("sensors/before")
      eventually { received.size == 1 }

      broker.drop!
      eventually { broker.transports.size == 2 }

      # the new connection must carry the subscription again
      eventually do
        broker.transports.last.sent_packets(Subscribe, MQTT::RequestType::Subscribe).any? do |sub|
          sub.topics.map(&.filter) == ["sensors/#"] && sub.topics.first.qos == QoS::BrokerReceived
        end
      end

      broker.publish("sensors/after")
      eventually { received.size == 2 }
      received.should eq ["sensors/before", "sensors/after"]

      client.disconnect
    end

    it "does not re-subscribe when the broker resumed the session" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)
      client.connect(clean_start: false)
      client.subscribe("sensors/#") { |_, _| nil }

      broker.session_present = true
      broker.drop!

      eventually { broker.transports.size == 2 }
      eventually { broker.transports.last.sent_types.includes?(MQTT::RequestType::Connect) }

      # give a stray SUBSCRIBE a chance to appear before asserting it did not
      sleep 50.milliseconds
      broker.transports.last.sent_types.should_not contain MQTT::RequestType::Subscribe

      client.disconnect
    end

    it "retries until the broker comes back" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)
      client.connect

      # the next two connection attempts fail before a transport even exists
      broker.refuse_connections = 2
      broker.drop!

      eventually(5.seconds) { broker.transports.size == 2 }
      client.terminated?.should be_false
      client.disconnect
    end

    it "gives up after max_attempts and terminates" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(
        reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds, max_attempts: 2)
      )
      client.connect

      closed = false
      spawn { client.wait_close; closed = true }

      broker.refuse_connections = 99
      broker.drop!

      eventually(5.seconds) { client.terminated? }
      eventually { closed }
    end

    it "does not reconnect after a deliberate disconnect" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)
      client.connect

      client.disconnect
      client.terminated?.should be_true

      sleep 50.milliseconds
      broker.transports.size.should eq 1
    end

    it "does not reconnect before a connection has ever been established" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(reconnect: FAST_RECONNECT)

      # dropped before `connect` was ever called, there is no session to resume
      broker.drop!

      eventually { client.terminated? }
      broker.transports.size.should eq 1
    end
  end

  describe MQTT::Reconnect do
    it "backs off exponentially up to max_delay" do
      policy = MQTT::Reconnect.new(initial_delay: 1.second, max_delay: 8.seconds)
      policy.delay_for(1).should eq 1.second
      policy.delay_for(2).should eq 2.seconds
      policy.delay_for(3).should eq 4.seconds
      policy.delay_for(4).should eq 8.seconds
      policy.delay_for(5).should eq 8.seconds
      # must not overflow on a connection that has been down for a long time
      policy.delay_for(1000).should eq 8.seconds
    end

    it "honours max_attempts" do
      MQTT::Reconnect.new(max_attempts: 3).give_up?(3).should be_false
      MQTT::Reconnect.new(max_attempts: 3).give_up?(4).should be_true
      MQTT::Reconnect.new.give_up?(10_000).should be_false
    end

    it "validates its delays" do
      expect_raises(ArgumentError, /initial_delay/) { MQTT::Reconnect.new(initial_delay: 0.seconds) }
      expect_raises(ArgumentError, /max_delay/) { MQTT::Reconnect.new(initial_delay: 5.seconds, max_delay: 1.second) }
    end
  end

  describe "transport lifecycle" do
    it "does not connect when the transport is constructed" do
      # nothing is listening, construction must still succeed
      transport = MQTT::Transport::TCP.new("127.0.0.1", 1, connect_timeout: 1)
      transport.closed?.should be_true
      transport.host.should eq "127.0.0.1"
    end

    it "reports a connection failure as an MQTT error" do
      transport = MQTT::Transport::TCP.new("127.0.0.1", 1, connect_timeout: 1)
      error = expect_raises(MQTT::NotConnectedError, /failed to establish/) do
        MQTT::V3::Client.new(transport)
      end
      error.cause.should_not be_nil
    end

    it "refuses to send before the transport has started" do
      transport = MQTT::Transport::TCP.new("127.0.0.1", 1, connect_timeout: 1)
      packet = MQTT::V3::EmptyPacket.new
      packet.id = MQTT::RequestType::Pingreq
      packet.packet_length = 0_u32

      expect_raises(MQTT::NotConnectedError, /not been started/) { transport.send(packet) }
    end
  end
end
