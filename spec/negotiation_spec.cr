require "./spec_helper"
require "./support/fake_negotiating_broker"
require "./support/fake_v5_broker"
require "../src/mqtt/client"

describe MQTT::Client do
  it "uses 5.0 when the broker speaks it" do
    broker = FakeNegotiatingBroker.new
    client = MQTT::Client.new(&broker.factory)

    client.connect(client_id: "negotiate").should eq MQTT::Version::V5
    client.version.should eq MQTT::Version::V5
    client.v5.should_not be_nil
    client.v3.should be_nil

    # only one attempt was needed
    broker.attempts.should eq [MQTT::Version::V5]
    client.disconnect
  end

  # a 5.0 broker that refuses this client answers 0x84 and closes
  it "falls back when the broker rejects 5.0 with an unsupported version reason" do
    broker = FakeNegotiatingBroker.new
    broker.behaviour = FakeNegotiatingBroker::Behaviour::RefusesFive
    client = MQTT::Client.new(&broker.factory)

    client.connect(client_id: "negotiate").should eq MQTT::Version::V311
    client.version.should eq MQTT::Version::V311
    client.v3.should_not be_nil
    client.v5.should be_nil

    broker.attempts.should eq [MQTT::Version::V5, MQTT::Version::V311]
    client.disconnect
  end

  # the harder case: a 3.1.1 broker replies with a 3.1.1 CONNACK, which the
  # 5.0 parser cannot decode, so the connection dies rather than reporting 0x84
  it "falls back when a 3.1.1 broker answers a 5.0 CONNECT in its own dialect" do
    broker = FakeNegotiatingBroker.new
    broker.behaviour = FakeNegotiatingBroker::Behaviour::LegacyOnly
    client = MQTT::Client.new(&broker.factory)

    client.connect(client_id: "negotiate").should eq MQTT::Version::V311
    client.version.should eq MQTT::Version::V311
    broker.attempts.first.should eq MQTT::Version::V5
    broker.attempts.last.should eq MQTT::Version::V311
    client.disconnect
  end

  it "publishes and subscribes over whichever version was negotiated" do
    {FakeNegotiatingBroker::Behaviour::Modern, FakeNegotiatingBroker::Behaviour::LegacyOnly}.each do |behaviour|
      broker = FakeNegotiatingBroker.new
      broker.behaviour = behaviour
      client = MQTT::Client.new(&broker.factory)
      client.connect(client_id: "common")

      received = [] of String
      client.subscribe("some/topic", qos: MQTT::QoS::BrokerReceived) do |topic, payload|
        received << "#{topic}=#{String.new(payload)}"
        nil
      end
      client.subscriptions.keys.should eq ["some/topic"]

      client.publish("some/topic", "hello", qos: MQTT::QoS::BrokerReceived)
      client.ping
      client.last_ping_response.should_not be_nil

      client.unsubscribe("some/topic")
      client.subscriptions.should be_empty
      client.disconnect
      client.closed?.should be_true
    end
  end

  it "reports the version before anything has been negotiated" do
    broker = FakeNegotiatingBroker.new
    client = MQTT::Client.new(&broker.factory)

    client.version.should be_nil
    client.closed?.should be_true
    expect_raises(MQTT::NotConnectedError, /connect has not been called/) { client.negotiated }
  end

  # the negotiated client keeps the factory, so a reconnect must not re-probe
  it "does not renegotiate when the connection drops" do
    broker = FakeNegotiatingBroker.new
    broker.behaviour = FakeNegotiatingBroker::Behaviour::LegacyOnly
    client = MQTT::Client.new(
      reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds),
      &broker.factory
    )
    client.connect(client_id: "negotiate").should eq MQTT::Version::V311
    attempts_after_connect = broker.attempts.size

    broker.transport.fail!(IO::Error.new("connection reset by peer"))

    eventually(5.seconds) { broker.attempts.size > attempts_after_connect }
    # every attempt after the first fallback is 3.1.1, never a fresh 5.0 probe
    broker.attempts[attempts_after_connect..].each(&.should(eq MQTT::Version::V311))
    client.disconnect
  end

  it "surfaces a refusal that is not about the version" do
    broker = FakeV5Broker.new
    broker.connack_reason = MQTT::V5::ReasonCode::BadUserNameOrPassword
    client = MQTT::Client.new { broker.build_transport.as(MQTT::Transport) }

    # not a version problem, so it must not silently retry as 3.1.1
    expect_raises(MQTT::ConnectError, /bad user name or password/) do
      client.connect(client_id: "negotiate")
    end
    client.version.should be_nil
  end
end
