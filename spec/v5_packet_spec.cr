require "./spec_helper"
require "../src/mqtt/v5/subscribe"
require "../src/mqtt/v5/connect"
require "../src/mqtt/v5/publish"

module MQTT::V5
  def self.round_trip(packet, klass : T.class) : T forall T
    IO::Memory.new(packet.to_slice).read_bytes(klass)
  end

  describe "V5 packets" do
    describe Ack do
      # MQTT-3.4.2.1, the reason code and properties are both omitted when the
      # reason is 0x00 with no properties. The remaining length is the only
      # thing that says so
      it "omits the reason code and properties on a plain success" do
        ack = Ack.new
        ack.id = MQTT::RequestType::Puback
        ack.message_id = 42_u16
        ack.packet_length = ack.calculate_length

        ack.calculate_length.should eq 2
        ack.to_slice.hexstring.should eq "4002002a"

        decoded = MQTT::V5.round_trip(ack, Ack)
        decoded.message_id.should eq 42_u16
        decoded.success?.should be_true
        decoded.reason_code.should eq ReasonCode::Success
      end

      # NoMatchingSubscribers is a success but is not 0x00, so it must still be
      # transmitted
      it "sends a non zero reason code even when it is a success" do
        ack = Ack.new
        ack.id = MQTT::RequestType::Puback
        ack.message_id = 42_u16
        ack.reason_code = ReasonCode::NoMatchingSubscribers
        ack.packet_length = ack.calculate_length

        ack.calculate_length.should eq 3
        ack.to_slice.hexstring.should eq "4003002a10"

        decoded = MQTT::V5.round_trip(ack, Ack)
        decoded.reason_code.should eq ReasonCode::NoMatchingSubscribers
        decoded.success?.should be_true
      end

      it "carries a reason string when the broker rejects the publish" do
        ack = Ack.new
        ack.id = MQTT::RequestType::Puback
        ack.message_id = 42_u16
        ack.reason_code = ReasonCode::QuotaExceeded
        ack.reason_string = "over quota"
        ack.packet_length = ack.calculate_length

        decoded = MQTT::V5.round_trip(ack, Ack)
        decoded.error?.should be_true
        decoded.reason_code.should eq ReasonCode::QuotaExceeded
        decoded.reason_string.should eq "over quota"
      end

      it "keeps an unrecognised reason code rather than raising" do
        raw = Bytes[0x40, 0x03, 0x00, 0x2A, 0x7A]
        decoded = IO::Memory.new(raw).read_bytes(Ack)

        decoded.reason_code.should be_nil
        decoded.raw_reason_code.should eq 0x7A_u8
        decoded.success?.should be_true
        decoded.reason_description.should contain "unknown reason code"
      end
    end

    describe Disconnect do
      it "encodes a normal disconnect as an empty packet" do
        packet = Disconnect.new
        packet.id = MQTT::RequestType::Disconnect
        packet.packet_length = packet.calculate_length
        packet.to_slice.hexstring.should eq "e000"
      end

      # a 5.0 broker can say why it is closing, which 3.1.1 could not express
      it "carries a reason when the broker initiates it" do
        packet = Disconnect.new
        packet.id = MQTT::RequestType::Disconnect
        packet.reason_code = ReasonCode::SessionTakenOver
        packet.packet_length = packet.calculate_length
        packet.to_slice.hexstring.should eq "e0018e"

        decoded = MQTT::V5.round_trip(packet, Disconnect)
        decoded.reason_code.should eq ReasonCode::SessionTakenOver
        decoded.error?.should be_true
      end

      it "carries a server reference for a redirect" do
        packet = Disconnect.new
        packet.id = MQTT::RequestType::Disconnect
        packet.reason_code = ReasonCode::ServerMoved
        packet.server_reference = "other.broker:1883"
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Disconnect)
        decoded.server_reference.should eq "other.broker:1883"
      end
    end

    describe Connect do
      it "round trips with properties" do
        packet = Connect.new
        packet.id = MQTT::RequestType::Connect
        packet.client_id = "abc"
        packet.clean_start = true
        packet.keep_alive_seconds = 30_u16
        packet.session_expiry_interval = 3600_u32
        packet.receive_maximum = 10_u16
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Connect)
        decoded.name.should eq "MQTT"
        decoded.version.should eq MQTT::Version::V5
        decoded.client_id.should eq "abc"
        decoded.keep_alive_seconds.should eq 30_u16
        decoded.session_expiry_interval.should eq 3600_u32
        decoded.receive_maximum.should eq 10_u16
        decoded.to_slice.should eq packet.to_slice
      end

      # the will has its own property section, and it comes before the topic
      it "round trips a will with its own properties" do
        packet = Connect.new
        packet.id = MQTT::RequestType::Connect
        packet.client_id = "abc"
        packet.will_flag = true
        packet.will_topic = "last/words"
        packet.will_payload = "goodbye"
        packet.will_delay_interval = 30_u32
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Connect)
        decoded.will_topic.should eq "last/words"
        String.new(decoded.will_payload).should eq "goodbye"
        decoded.will_delay_interval.should eq 30_u32
      end

      it "rejects a property that does not belong on a CONNECT" do
        packet = Connect.new
        packet.properties.set_two_byte(PropertyId::TopicAlias, 1_u16)

        expect_raises(MQTT::ProtocolError, /not a valid property for CONNECT/) { packet.validate! }
      end
    end

    describe Connack do
      it "exposes the broker's negotiated limits" do
        packet = Connack.new
        packet.id = MQTT::RequestType::Connack
        packet.session_present = true
        packet.properties.set_two_byte(PropertyId::ServerKeepAlive, 45_u16)
        packet.properties.set_byte(PropertyId::MaximumQoS, 1_u8)
        packet.properties.set_byte(PropertyId::RetainAvailable, 0_u8)
        packet.properties.set_string(PropertyId::AssignedClientIdentifier, "auto-123")
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Connack)
        decoded.session_present.should be_true
        decoded.server_keep_alive.should eq 45_u16
        decoded.maximum_qos.should eq QoS::BrokerReceived
        decoded.retain_available?.should be_false
        decoded.assigned_client_identifier.should eq "auto-123"
      end

      # MQTT-3.2.2.3, an absent capability means available
      it "defaults every capability to available when the broker is silent" do
        packet = Connack.new
        packet.receive_maximum.should eq 65_535_u16
        packet.maximum_qos.should eq QoS::SubscribersReceived
        packet.retain_available?.should be_true
        packet.wildcard_subscription_available?.should be_true
        packet.subscription_identifier_available?.should be_true
        packet.shared_subscription_available?.should be_true
        packet.topic_alias_maximum.should eq 0_u16
      end

      it "raises a scoped error when the broker refuses" do
        packet = Connack.new
        packet.id = MQTT::RequestType::Connack
        packet.reason_code = ReasonCode::BadUserNameOrPassword

        error = expect_raises(MQTT::ConnectError, /bad user name or password/) { packet.success! }
        error.return_code.should eq 0x86_u8
      end
    end

    describe Subscribe do
      it "round trips the subscription options byte" do
        packet = Subscribe.new
        packet.id = MQTT::RequestType::Subscribe
        packet.qos = QoS::BrokerReceived
        packet.message_id = 7_u16
        packet.subscription_identifier = 99_u32

        topic = SubTopic.new
        topic.filter = "sensors/#"
        topic.qos = QoS::BrokerReceived
        topic.no_local = true
        topic.retain_as_published = true
        topic.retain_handling = RetainHandling::Never
        packet.topics = [topic]
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Subscribe)
        decoded.subscription_identifier.should eq 99_u32

        first = decoded.topics.first
        first.filter.should eq "sensors/#"
        first.qos.should eq QoS::BrokerReceived
        first.no_local.should be_true
        first.retain_as_published.should be_true
        first.retain_handling.should eq RetainHandling::Never
      end

      it "defaults to sending retained messages on subscribe" do
        SubTopic.new.retain_handling.should eq RetainHandling::SendAlways
        SubTopic.new.no_local.should be_false
        SubTopic.new.retain_as_published.should be_false
      end

      it "reads several filters back in order" do
        packet = Subscribe.new
        packet.id = MQTT::RequestType::Subscribe
        packet.qos = QoS::BrokerReceived
        packet.message_id = 1_u16
        packet.topics = {"a/b" => QoS::FireAndForget, "c/d" => QoS::SubscribersReceived}
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Subscribe)
        decoded.topic_hash.should eq({"a/b" => QoS::FireAndForget, "c/d" => QoS::SubscribersReceived})
      end
    end

    describe Suback do
      it "returns a reason code per filter" do
        packet = Suback.new
        packet.id = MQTT::RequestType::Suback
        packet.message_id = 7_u16
        packet.reason_codes = [ReasonCode::GrantedQoS1, ReasonCode::UnspecifiedError]
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Suback)
        decoded.reason_codes.should eq [ReasonCode::GrantedQoS1, ReasonCode::UnspecifiedError]
        decoded.raw_reason_codes.map { |code| decoded.failure?(code) }.should eq [false, true]
        decoded.reason_codes.first.try(&.granted_qos).should eq QoS::BrokerReceived
      end
    end

    describe Publish do
      it "round trips with the 5.0 property section" do
        packet = Publish.new
        packet.id = MQTT::RequestType::Publish
        packet.qos = QoS::BrokerReceived
        packet.topic = "sensors/temp"
        packet.message_id = 3_u16
        packet.payload = "21.5"
        packet.content_type = "text/plain"
        packet.response_topic = "reply/here"
        packet.correlation_data = Bytes[9, 9]
        packet.topic_alias = 4_u16
        packet.add_user_property("unit", "celsius")
        packet.packet_length = packet.calculate_length

        decoded = MQTT::V5.round_trip(packet, Publish)
        decoded.topic.should eq "sensors/temp"
        String.new(decoded.payload).should eq "21.5"
        decoded.content_type.should eq "text/plain"
        decoded.response_topic.should eq "reply/here"
        decoded.correlation_data.should eq Bytes[9, 9]
        decoded.topic_alias.should eq 4_u16
        decoded.user_properties.should eq [{"unit", "celsius"}]
        decoded.to_slice.should eq packet.to_slice
      end

      # MQTT-3.3.2.3.4
      it "refuses a zero topic alias" do
        packet = Publish.new
        expect_raises(ArgumentError, /greater than zero/) { packet.topic_alias = 0_u16 }
      end

      it "collects the subscription identifiers that matched" do
        packet = Publish.new
        packet.id = MQTT::RequestType::Publish
        packet.topic = "a/b"
        packet.payload = "x"
        packet.add_subscription_identifier(1_u32)
        packet.add_subscription_identifier(300_u32)
        packet.packet_length = packet.calculate_length

        MQTT::V5.round_trip(packet, Publish).subscription_identifiers.should eq [1_u32, 300_u32]
      end

      # the same UInt32 underflow the V3 packet was hardened against
      it "rejects a remaining length too small for its own fields" do
        expect_raises(Exception) do
          IO::Memory.new(Bytes[0x30, 0x02, 0x00, 0x08, 0x00]).read_bytes(Publish)
        end
      end
    end

    describe ReasonCode do
      it "treats everything below 0x80 as a success" do
        ReasonCode::Success.success?.should be_true
        ReasonCode::GrantedQoS2.success?.should be_true
        ReasonCode::NoMatchingSubscribers.success?.should be_true
        ReasonCode::UnspecifiedError.success?.should be_false
        ReasonCode::WildcardSubscriptionsNotSupported.error?.should be_true
      end

      it "maps a granted code back to a QoS" do
        ReasonCode::Success.granted_qos.should eq QoS::FireAndForget
        ReasonCode::GrantedQoS1.granted_qos.should eq QoS::BrokerReceived
        ReasonCode::GrantedQoS2.granted_qos.should eq QoS::SubscribersReceived
        ReasonCode::QuotaExceeded.granted_qos.should be_nil
      end
    end
  end
end
