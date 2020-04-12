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

      it "should parse a simple 3.1.0 Connect packet" do
        io = combine(Bytes[0x10, 0x16, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0x00, 0x00, 0x0a, 0x00, 0x08], "myclient")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(false)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a simple 3.1.1 Connect packet" do
        io = combine(Bytes[0x10, 0x14, 0x00, 0x04], "MQTT", Bytes[0x04, 0x00, 0x00, 0x0a, 0x00, 0x08], "myclient")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQTT")
          packet.version.should eq(Version::V311)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(false)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with the clean session flag set" do
        io = combine(Bytes[0x10, 0x16, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0x02, 0x00, 0x0a, 0x00, 0x08], "myclient")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(true)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with a Will and Testament" do
        io = combine(Bytes[0x10, 0x24, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0x0e, 0x00, 0x0a, 0x00, 0x08], "myclient", Bytes[0x00, 0x05], "topic", Bytes[0x00, 0x05], "hello")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(true)
          packet.will_topic.should eq("topic")
          packet.will_payload.should eq("hello")
          packet.will_flag.should eq(true)
          packet.will_qos.should eq(QoS::BrokerReceived)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with a username and password" do
        io = combine(Bytes[0x10, 0x2a, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0xc0, 0x00, 0x0a, 0x00, 0x08], "myclient", Bytes[0x00, 0x08], "username", Bytes[0x00, 0x08], "password")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(false)
          packet.username.should eq("username")
          packet.password.should eq("password")
          packet.will_flag.should eq(false)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with a username and no password" do
        io = combine(Bytes[0x10, 0x20, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0x80, 0x00, 0x0a, 0x00, 0x08], "myclient", Bytes[0x00, 0x08], "username")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(false)
          packet.username.should eq("username")
          packet.password.should eq("")
          packet.will_flag.should eq(false)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with a password and no username" do
        io = combine(Bytes[0x10, 0x20, 0x00, 0x06], "MQIsdp", Bytes[0x03, 0x40, 0x00, 0x0a, 0x00, 0x08], "myclient", Bytes[0x00, 0x08], "password")

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("myclient")
          packet.keep_alive_seconds.should eq(10)
          packet.clean_start.should eq(false)
          packet.username.should eq("")
          packet.password.should eq("password")
          packet.will_flag.should eq(false)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a Connect packet with every option set" do
        io = combine(
          Bytes[0x10, 0x5f, 0x00, 0x06], "MQIsdp",
          Bytes[0x03, 0xf6, 0xff, 0xff, 0x00, 0x17], "12345678901234567890123",
          Bytes[0x00, 0x0a], "will_topic", Bytes[0x00, 0x0c], "will_message",
          Bytes[0x00, 0x0e], "user0123456789", Bytes[0x00, 0x0e], "pass0123456789"
        )

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connect
          packet = io.read_bytes Connect
          packet.qos.should eq(QoS::FireAndForget)
          packet.name.should eq("MQIsdp")
          packet.version.should eq(Version::V31)
          packet.client_id.should eq("12345678901234567890123")
          packet.keep_alive_seconds.should eq(65535)
          packet.username.should eq("user0123456789")
          packet.password.should eq("pass0123456789")
          packet.clean_start.should eq(true)
          packet.will_topic.should eq("will_topic")
          packet.will_payload.should eq("will_message")
          packet.will_flag.should eq(true)
        else
          raise "incorrect packet type"
        end
      end
    end

    describe Connack do
      it "should output the correct bytes for a sucessful connection acknowledgement packet without Session Present set" do
        packet = new_connack_packet
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x20, 0x02, 0x00, 0x00]).to_slice)
      end

      it "should output the correct bytes for a sucessful connection acknowledgement packet with Session Present set" do
        packet = new_connack_packet
        packet.session_present = true
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x20, 0x02, 0x01, 0x00]).to_slice)
      end

      it "should parse a successful Connection Accepted packet" do
        io = combine(Bytes[0x20, 0x02, 0x00, 0x00])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connack
          packet = io.read_bytes Connack
          packet.session_present.should eq(false)
          packet.return_code.should eq(0)
          packet.success?.should eq(true)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a successful Connection Accepted packet with Session Present set" do
        io = combine(Bytes[0x20, 0x02, 0x01, 0x00])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connack
          packet = io.read_bytes Connack
          packet.session_present.should eq(true)
          packet.return_code.should eq(0)
          packet.success?.should eq(true)
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a client identifier rejected packet" do
        io = combine(Bytes[0x20, 0x02, 0x00, 0x02])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Connack
          packet = io.read_bytes Connack
          packet.session_present.should eq(false)
          packet.return_code.should eq(2)
          packet.success?.should eq(false)

          expect_raises(::Exception, "Connection refused: client identifier rejected") do
            packet.success!
          end
        else
          raise "incorrect packet type"
        end
      end
    end

    describe Puback do
      it "should output the correct bytes for a sucessful connection acknowledgement packet without Session Present set" do
        packet = MQTT::V3::Puback.new
        packet.header.id = MQTT::RequestType::Puback
        packet.message_id = 0x1234_u16
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(Bytes[0x40, 0x02, 0x12, 0x34]).to_slice)
      end

      it "should parse a packet" do
        io = combine(Bytes[0x40, 0x02, 0x12, 0x34])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Puback
          packet = io.read_bytes Puback
          packet.message_id.should eq(0x1234)
        else
          raise "incorrect packet type"
        end
      end
    end

    describe Subscribe do
      it "should output the correct bytes for a packet with a single topic" do
        packet = MQTT::V3::Subscribe.new
        packet.header.id = MQTT::RequestType::Subscribe
        packet.qos = QoS::BrokerReceived
        packet.message_id = 1_u16
        packet.topic = "a/b"
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(
          Bytes[0x82, 0x08, 0x00, 0x01, 0x00, 0x03], "a/b", Bytes[0x00]
        ).to_slice)
      end

      it "should output the correct bytes for a packet with multiple topics" do
        packet = MQTT::V3::Subscribe.new
        packet.header.id = MQTT::RequestType::Subscribe
        packet.qos = QoS::BrokerReceived
        packet.message_id = 6_u16
        packet.topics = {
          "a/b" => QoS::FireAndForget,
          "c/d" => QoS::BrokerReceived,
        }
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(
          Bytes[0x82, 0x0e, 0x00, 0x06, 0x00, 0x03],
          "a/b", Bytes[0x00, 0x00, 0x03], "c/d", Bytes[0x01]
        ).to_slice)
      end

      it "should parse a packet with a single topic" do
        io = combine(Bytes[0x82, 0x08, 0x00, 0x01, 0x00, 0x03], "a/b", Bytes[0x00])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Subscribe
          packet = io.read_bytes Subscribe
          packet.message_id.should eq(1)
          packet.topic_hash.should eq({
            "a/b" => QoS::FireAndForget,
          })
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a packet with two topics" do
        io = combine(Bytes[0x82, 0x0e, 0x00, 0x06, 0x00, 0x03], "a/b", Bytes[0x00], Bytes[0x00, 0x03], "c/d", Bytes[0x01])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Subscribe
          packet = io.read_bytes Subscribe
          packet.message_id.should eq(6)
          packet.topic_hash.should eq({
            "a/b" => QoS::FireAndForget,
            "c/d" => QoS::BrokerReceived,
          })
        else
          raise "incorrect packet type"
        end
      end
    end

    describe Suback do
      it "should output the correct bytes for an acknowledgement to a single topic" do
        packet = MQTT::V3::Suback.new
        packet.header.id = MQTT::RequestType::Suback
        packet.message_id = 5_u16
        packet.return_codes = [QoS::FireAndForget]
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(
          Bytes[0x90, 0x03, 0x00, 0x05, 0x00]
        ).to_slice)
      end

      it "should output the correct bytes for an acknowledgement for two topics" do
        packet = MQTT::V3::Suback.new
        packet.header.id = MQTT::RequestType::Suback
        packet.message_id = 6_u16
        packet.return_codes = [QoS::FireAndForget, QoS::BrokerReceived]
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(
          Bytes[0x90, 0x04, 0x00, 0x06, 0x00, 0x01]
        ).to_slice)
      end

      it "should parse a packet with a topic" do
        io = combine(Bytes[0x90, 0x03, 0x12, 0x34, 0x00])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Suback
          packet = io.read_bytes Suback
          packet.message_id.should eq(0x1234)
          packet.return_codes.should eq([
            QoS::FireAndForget,
          ])
        else
          raise "incorrect packet type"
        end
      end

      it "should parse a packet with two topics" do
        io = combine(Bytes[0x90, 0x04, 0x12, 0x34, 0x01, 0x01])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Suback
          packet = io.read_bytes Suback
          packet.message_id.should eq(0x1234)
          packet.return_codes.should eq([
            QoS::BrokerReceived, QoS::BrokerReceived,
          ])
        else
          raise "incorrect packet type"
        end
      end
    end

    describe Pingreq do
      it "should output the correct bytes for a packet with no flags" do
        packet = MQTT::V3::Pingreq.new
        packet.header.id = MQTT::RequestType::Pingreq
        packet.header.packet_length = packet.packet_length

        packet.to_slice.should eq(combine(
          Bytes[0xc0, 0x00]
        ).to_slice)
      end

      it "should parse a packet" do
        io = combine(Bytes[0xc0, 0x00])

        # Grab the packet type
        case MQTT.peek_type(io)
        when RequestType::Pingreq
          io.read_bytes Pingreq
        else
          raise "incorrect packet type"
        end
      end
    end
  end # describe MQTT::V3
end   # MQTT::V3
