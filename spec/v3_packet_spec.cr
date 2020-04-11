require "./spec_helper"

module MQTT::V3
  describe MQTT::V3 do
    describe "when serialising a packet" do
      it "should output the correct bytes for a packet with default QoS and no flags" do
        packet = new_publish_packet
        packet.topic = "test"
        packet.payload = "hello world"
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(Bytes[0x30, 0x11, 0x00, 0x04], "testhello world").to_slice)
      end

      it "should output the correct bytes for a packet with QoS 1 and no flags" do
        packet = new_publish_packet
        packet.message_id = 5_u16
        packet.qos = QoS::BrokerReceived
        packet.topic = "a/b"
        packet.payload = "hello world"
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x32, 0x12, 0x00, 0x03], "a/b", Bytes[0x00, 0x05], "hello world").to_slice)
      end

      it "should output the correct bytes for a packet with QoS 2 and retain flag set" do
        packet = new_publish_packet
        packet.message_id = 5_u16
        packet.qos = QoS::SubscribersReceived
        packet.retain = true
        packet.topic = "c/d"
        packet.payload = "hello world"
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x35, 0x12, 0x00, 0x03], "c/d", Bytes[0x00, 0x05], "hello world").to_slice)
      end

      it "should output the correct bytes for a packet with QoS 2 and dup flag set" do
        packet = new_publish_packet
        packet.message_id = 5_u16
        packet.qos = QoS::SubscribersReceived
        packet.duplicate = true
        packet.topic = "c/d"
        packet.payload = "hello world"
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x3C, 0x12, 0x00, 0x03], "c/d", Bytes[0x00, 0x05], "hello world").to_slice)
      end

      it "should output the correct bytes for a packet with an empty payload" do
        packet = new_publish_packet
        packet.topic = "test"
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(Bytes[0x30, 0x06, 0x00, 0x04], "test").to_slice)
      end
    end

    it "should parse a packet with QoS 0" do
      io = combine(Bytes[0x30, 0x11, 0x00, 0x04], "testhello world")

      # Grab the packet type
      case MQTT.peek_type(io)
      when RequestType::Publish
        packet = io.read_bytes Publish
        packet.qos.should eq(QoS::FireAndForget)
        packet.retain.should eq(false)
        packet.duplicate.should eq(false)
        packet.topic.should eq("test")
        String.new(packet.payload).should eq("hello world")
      else
        raise "incorrect packet type"
      end
    end

    it "should parse a packet with QoS 2, retain and dup flags set" do
      io = combine(Bytes[0x3D, 0x12, 0x00, 0x03], "c/d", Bytes[0x00, 0x05], "hello world")

      # Grab the packet type
      case MQTT.peek_type(io)
      when RequestType::Publish
        packet = io.read_bytes Publish
        packet.qos.should eq(QoS::SubscribersReceived)
        packet.retain.should eq(true)
        packet.duplicate.should eq(true)
        packet.topic.should eq("c/d")
        String.new(packet.payload).should eq("hello world")
      else
        raise "incorrect packet type"
      end
    end

    it "should parse a packet with an empty payload" do
      io = combine(Bytes[0x30, 0x06, 0x00, 0x04], "test")

      # Grab the packet type
      case MQTT.peek_type(io)
      when RequestType::Publish
        packet = io.read_bytes Publish
        packet.qos.should eq(QoS::FireAndForget)
        packet.topic.should eq("test")
        String.new(packet.payload).should eq("")
      else
        raise "incorrect packet type"
      end
    end

    it "should parse a packet with a body of 314 bytes" do
      io = combine(Bytes[0x30, 0xC1, 0x02, 0x00, 0x05], "topic", "x" * 314)

      # Grab the packet type
      case MQTT.peek_type(io)
      when RequestType::Publish
        packet = io.read_bytes Publish
        packet.qos.should eq(QoS::FireAndForget)
        packet.topic.should eq("topic")
        packet.payload.size.should eq(314)
      else
        raise "incorrect packet type"
      end
    end

    it "should parse a packet with a body of 16kbytes" do
      io = combine(Bytes[0x30, 0x87, 0x80, 0x01, 0x00, 0x05], "topic", "x" * 16384)

      # Grab the packet type
      case MQTT.peek_type(io)
      when RequestType::Publish
        packet = io.read_bytes Publish
        packet.qos.should eq(QoS::FireAndForget)
        packet.topic.should eq("topic")
        packet.payload.size.should eq(16384)
      else
        raise "incorrect packet type"
      end
    end

    it "should process a packet containing UTF-8 characters" do
      packet = new_publish_packet
      packet.topic = "Test ①"
      packet.payload = "Snowman: ☃"
      packet.header.packet_length = packet.packet_length
      packet.to_slice.should eq(combine(Bytes[0x30, 0x16, 0x00, 0x08], "Test ", Bytes[0xE2, 0x91, 0xA0], "Snowman: ", Bytes[0xE2, 0x98, 0x83]).to_slice)
    end

    describe Connect do
      it "should output the correct bytes for a packet with no flags" do
        packet = new_connect_packet(Version::V31, "myclient")
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(
          Bytes[0x10, 0x16, 0x00, 0x06], "MQIsdp",
          Bytes[0x03, 0x02, 0x00, 0x0f, 0x00, 0x08], "myclient"
        ).to_slice)
      end

      it "should output the correct bytes for a packet with a will" do
        packet = new_connect_packet(Version::V31, "myclient")
        packet.will_qos = QoS::BrokerReceived
        packet.will_topic = "topic"
        packet.will_payload = "hello"
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(
          Bytes[0x10, 0x24, 0x00, 0x06], "MQIsdp",
          Bytes[0x03, 0x0e, 0x00, 0x0f, 0x00, 0x08], "myclient",
          Bytes[0x00, 0x05], "topic", Bytes[0x00, 0x05], "hello",
        ).to_slice)
      end

      it "should output the correct bytes for a packet with a username and password" do
        packet = new_connect_packet(Version::V31, "myclient")
        packet.username = "username"
        packet.password = "password"
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(
          Bytes[0x10, 0x2A, 0x00, 0x06], "MQIsdp",
          Bytes[0x03, 0xC2, 0x00, 0x0f, 0x00, 0x08], "myclient",
          Bytes[0x00, 0x08], "username", Bytes[0x00, 0x08], "password",
        ).to_slice)
      end

      it "should output the correct bytes for a packet with everything" do
        packet = new_connect_packet(Version::V31, "12345678901234567890123")
        packet.will_qos = QoS::SubscribersReceived
        packet.will_topic = "will_topic"
        packet.will_payload = "will_message"
        packet.will_retain = true
        packet.keep_alive_seconds = 65535
        packet.username = "user0123456789"
        packet.password = "pass0123456789"
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(
          Bytes[0x10, 0x5F, 0x00, 0x06], "MQIsdp",
          Bytes[0x03, 0xf6, 0xff, 0xff, 0x00, 0x17], "12345678901234567890123",
          Bytes[0x00, 0x0A], "will_topic", Bytes[0x00, 0x0C], "will_message",
          Bytes[0x00, 0x0E], "user0123456789", Bytes[0x00, 0x0E], "pass0123456789",
        ).to_slice)
      end

      it "should output the correct bytes for a 3.1.1 packet with no flags" do
        packet = new_connect_packet(Version::V311, "myclient")
        packet.header.packet_length = packet.packet_length
        packet.to_slice.should eq(combine(
          Bytes[0x10, 0x14, 0x00, 0x04], "MQTT",
          Bytes[0x04, 0x02, 0x00, 0x0f, 0x00, 0x08], "myclient"
        ).to_slice)
      end
    end
  end # describe MQTT::V3
end   # MQTT::V3
