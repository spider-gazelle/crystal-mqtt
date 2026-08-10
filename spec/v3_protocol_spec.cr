require "./spec_helper"

# Test only access to the packet identifier counter
class MQTT::V3::Client
  def probe_message_id=(value : UInt16)
    @message_id = value
  end

  def probe_next_message_id : UInt16
    @message_lock.synchronize { next_message_id }
  end
end

module MQTT::V3
  describe "protocol conformance" do
    describe Header do
      it "round trips a multi byte remaining length" do
        header = Header.new
        header.id = MQTT::RequestType::Publish
        header.packet_length = 300_u32

        decoded = IO::Memory.new(header.to_slice).read_bytes(Header)
        decoded.packet_length.should eq 300_u32
        decoded.fixed_header_size.should eq 2 + 1
      end

      # H6 regression, this used to wrap silently
      it "refuses a remaining length the header cannot encode" do
        header = Header.new
        header.id = MQTT::RequestType::Publish

        expect_raises(MQTT::PacketError, /exceeds the maximum/) do
          header.packet_length = MQTT::MAX_REMAINING_LENGTH + 1
        end

        header.packet_length = MQTT::MAX_REMAINING_LENGTH
        header.packet_length.should eq MQTT::MAX_REMAINING_LENGTH
      end

      # H6 regression, the decoded length was cached and never invalidated
      it "recalculates the length when the variable length fields are written" do
        header = Header.new
        header.id = MQTT::RequestType::Publish
        header.packet_length = 5_u32

        header.variable_length1 = 10_u8
        header.packet_length.should eq 10_u32
      end

      it "reports the fixed header size without serialising" do
        header = Header.new
        header.id = MQTT::RequestType::Publish

        {0_u32 => 2, 127_u32 => 2, 128_u32 => 3, 16_383_u32 => 3, 16_384_u32 => 4}.each do |length, expected|
          header.packet_length = length
          header.fixed_header_size.should eq expected
          header.to_slice.size.should eq expected
        end
      end
    end

    describe Publish do
      # H3 regression, this underflowed UInt32 into a ~4GB allocation
      it "rejects a remaining length smaller than its own topic field" do
        expect_raises(Exception) do
          IO::Memory.new(Bytes[0x30, 0x01, 0x00, 0x00]).read_bytes(Publish)
        end
      end
    end

    describe Connack do
      it "raises a scoped error when the broker refuses the connection" do
        packet = Connack.new
        packet.id = MQTT::RequestType::Connack
        packet.return_code = 4_u8

        error = expect_raises(MQTT::ConnectError, /bad user name or password/) { packet.success! }
        error.return_code.should eq 4_u8
        error.should be_a MQTT::Error
      end
    end

    describe MQTT do
      # L5 regression, this raised a bare ArgumentError
      it "raises a scoped error for a reserved packet type" do
        expect_raises(MQTT::ProtocolError, /reserved packet type/) do
          MQTT.peek_type(IO::Memory.new(Bytes[0x00, 0x00]))
        end
      end

      it "raises a scoped error when there is no data to peek at" do
        expect_raises(MQTT::ProtocolError, /no data available/) do
          MQTT.peek_type(IO::Memory.new(Bytes.empty))
        end
      end
    end

    describe "packet identifiers" do
      # M2 regression, MQTT-2.3.1-1 reserves 0
      it "skips zero when the counter wraps" do
        broker = FakeBroker.new
        client = broker.client
        client.probe_message_id = 0xFFFE_u16

        client.probe_next_message_id.should eq 0xFFFF_u16
        # 0 is reserved, so the counter has to step over it
        client.probe_next_message_id.should eq 1_u16
      end
    end
  end
end
