require "./spec_helper"
require "./support/fake_v5_broker"

module MQTT::V5
  # Builds an authenticator that records what it was challenged with
  def self.recording(seen : Array(String), method = "SCRAM-SHA-1") : Client::Authenticator
    Client::Authenticator.new(method) do |data|
      seen << (data ? String.new(data) : "<initial>")
      data ? "response-to-#{String.new(data)}".to_slice : "initial-response".to_slice
    end
  end

  describe "enhanced authentication" do
    it "names the method and sends the first step on CONNECT" do
      broker = FakeV5Broker.new
      client = broker.client
      seen = [] of String
      client.authenticator = MQTT::V5.recording(seen)

      client.connect(client_id: "auth").success?.should be_true

      connect = broker.all_sent_packets(Connect, MQTT::RequestType::Connect).first
      connect.authentication_method.should eq "SCRAM-SHA-1"
      initial = connect.authentication_data
      raise "CONNECT carried no authentication data" unless initial
      String.new(initial).should eq "initial-response"
      seen.should eq ["<initial>"]
      client.disconnect
    end

    # MQTT-4.12, the broker may challenge several times before the CONNACK
    it "answers a multi step challenge before the connection completes" do
      broker = FakeV5Broker.new
      broker.auth_rounds = 3
      client = broker.client
      seen = [] of String
      client.authenticator = MQTT::V5.recording(seen)

      client.connect(client_id: "auth").success?.should be_true

      # initial, then one per challenge
      seen.size.should eq 4
      seen.first.should eq "<initial>"
      seen[1..].should eq ["challenge-3", "challenge-2", "challenge-1"]

      sent = broker.all_sent_types
      sent.count(MQTT::RequestType::Auth).should eq 3
      auths = broker.all_sent_packets(Auth, MQTT::RequestType::Auth)
      auths.each { |packet| packet.reason_code.should eq ReasonCode::ContinueAuthentication }
      auths.each { |packet| packet.authentication_method.should eq "SCRAM-SHA-1" }
      client.disconnect
    end

    it "surfaces a broker that refuses the credentials" do
      broker = FakeV5Broker.new
      broker.connack_reason = ReasonCode::BadAuthenticationMethod
      client = broker.client
      client.authenticator = MQTT::V5.recording([] of String)

      ack = client.connect(client_id: "auth")
      ack.success?.should be_false
      ack.reason_code.should eq ReasonCode::BadAuthenticationMethod
      expect_raises(MQTT::ConnectError, /bad authentication method/) { ack.success! }
      client.closed?.should be_true
    end

    # MQTT-4.12.1, re-authentication mid connection
    it "re-authenticates an established connection" do
      broker = FakeV5Broker.new
      broker.auth_rounds = 2
      client = broker.client
      seen = [] of String
      client.authenticator = MQTT::V5.recording(seen)
      client.connect(client_id: "auth").success?.should be_true

      before = seen.size
      client.reauthenticate

      # a fresh initial step, then the challenges
      seen.size.should be > before
      seen[before].should eq "<initial>"

      reauth = broker.all_sent_packets(Auth, MQTT::RequestType::Auth)
        .find { |packet| packet.reason_code == ReasonCode::ReAuthenticate }
      reauth.should_not be_nil
      client.disconnect
    end

    it "raises when re-authenticating without an authenticator" do
      broker = FakeV5Broker.new
      client = broker.client
      client.connect(client_id: "auth")

      expect_raises(MQTT::Error, /no authenticator/) { client.reauthenticate }
      client.disconnect
    end

    it "times out rather than hanging when the broker never finishes" do
      broker = FakeV5Broker.new
      client = broker.client(timeout: 100.milliseconds)
      client.authenticator = MQTT::V5.recording([] of String)
      client.connect(client_id: "auth")

      broker.silent_auth = true
      expect_raises(MQTT::TimeoutError) { client.reauthenticate }
      client.disconnect
    end

    it "closes the connection when challenged with no authenticator configured" do
      broker = FakeV5Broker.new
      broker.auth_rounds = 1
      client = broker.client(timeout: 200.milliseconds)

      # the broker challenges but we have nothing to answer with
      expect_raises(MQTT::Error) { client.connect(client_id: "auth") }
      eventually { client.closed? }
    end
  end

  describe "server initiated disconnect" do
    it "records the reason the broker gave" do
      broker = FakeV5Broker.new
      client = broker.client
      client.connect(client_id: "disc")

      broker.disconnect!(ReasonCode::SessionTakenOver)
      eventually { client.closed? }
      client.disconnect_reason.should eq ReasonCode::SessionTakenOver
    end

    it "follows a server reference on a redirect" do
      broker = FakeV5Broker.new
      client = broker.client
      client.connect(client_id: "disc")

      broker.disconnect!(ReasonCode::ServerMoved, server_reference: "other.broker:1883")
      eventually { client.closed? }
      client.server_reference.should eq "other.broker:1883"
    end

    # the reason that bites in practice: a fixed client id means a redeploy or a
    # duplicate publisher takes the session over. Reconnecting turns that into
    # two clients kicking each other off indefinitely
    it "does not reconnect after the session was taken over" do
      broker = FakeV5Broker.new
      client = MQTT::V5::Client.new(
        reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds)
      ) { broker.build_transport }
      client.connect(client_id: "shared")
      broker.transports.size.should eq 1

      broker.disconnect!(ReasonCode::SessionTakenOver)
      eventually { client.terminated? }
      sleep 50.milliseconds
      broker.transports.size.should eq 1
      client.disconnect_reason.should eq ReasonCode::SessionTakenOver
    end

    # a broker that says not to come back should not be retried
    it "does not reconnect after a fatal reason" do
      broker = FakeV5Broker.new
      client = MQTT::V5::Client.new(
        reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds)
      ) { broker.build_transport }
      client.connect(client_id: "disc")
      broker.transports.size.should eq 1

      broker.disconnect!(ReasonCode::Banned)
      eventually { client.terminated? }
      sleep 50.milliseconds
      broker.transports.size.should eq 1
    end

    it "does reconnect after a reason that is worth retrying" do
      broker = FakeV5Broker.new
      client = MQTT::V5::Client.new(
        reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds)
      ) { broker.build_transport }
      client.connect(client_id: "disc")

      broker.disconnect!(ReasonCode::ServerShuttingDown)
      eventually(5.seconds) { broker.transports.size == 2 }
      client.terminated?.should be_false
      client.disconnect
    end
  end

  describe "reason codes on acknowledgements" do
    it "raises when the broker rejects a QoS 1 publish" do
      broker = FakeV5Broker.new
      broker.puback_reason = ReasonCode::QuotaExceeded
      client = broker.client
      client.connect(client_id: "ack")

      expect_raises(MQTT::ProtocolError, /quota exceeded/) do
        client.publish("some/topic", "payload", qos: QoS::BrokerReceived)
      end
      client.disconnect
    end

    # MQTT-4.3.3, a failing PUBREC ends the exchange without a PUBREL
    it "does not send PUBREL when PUBREC reports a failure" do
      broker = FakeV5Broker.new
      broker.pubrec_reason = ReasonCode::NotAuthorized
      client = broker.client
      client.connect(client_id: "ack")

      expect_raises(MQTT::ProtocolError, /not authorized/) do
        client.publish("some/topic", "payload", qos: QoS::SubscribersReceived)
      end
      broker.all_sent_types.should_not contain MQTT::RequestType::Pubrel
      client.disconnect
    end

    it "keeps publishing after a rejected message, the slot is returned" do
      broker = FakeV5Broker.new
      broker.receive_maximum = 1_u16
      client = broker.client
      client.connect(client_id: "ack")

      broker.puback_reason = ReasonCode::QuotaExceeded
      expect_raises(MQTT::ProtocolError) { client.publish("a/topic", "x", qos: QoS::BrokerReceived) }

      # with a single in flight slot, a leak here would deadlock the next publish
      broker.puback_reason = ReasonCode::Success
      client.publish("a/topic", "y", qos: QoS::BrokerReceived)
      client.disconnect
    end

    it "names the filters the broker rejected" do
      broker = FakeV5Broker.new
      broker.suback_reasons = [ReasonCode::GrantedQoS1, ReasonCode::TopicFilterInvalid]
      client = broker.client
      client.connect(client_id: "sub")

      expect_raises(MQTT::SubscriptionError, /b\/topic/) do
        client.subscribe("a/topic", "b/topic", qos: QoS::BrokerReceived) { |_, _| nil }
      end
      client.disconnect
    end
  end
end
