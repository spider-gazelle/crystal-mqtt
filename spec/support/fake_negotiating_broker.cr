require "./fake_transport"
require "../../src/mqtt/v5/client"
require "../../src/mqtt/v3/client"

# A broker that answers according to the protocol version the CONNECT asked
# for, so version negotiation can be driven without a real 3.1.1 only broker.
#
# The awkward case it exists to model: a 3.1.1 broker rejects a 5.0 CONNECT
# with a **3.1.1 shaped** CONNACK, which a 5.0 parser cannot decode. The client
# has to recover from that, not just from a clean 0x84 reason code.
class FakeNegotiatingBroker
  enum Behaviour
    # speaks 5.0, accepts everything
    Modern
    # speaks 5.0 but refuses this client with 0x84
    RefusesFive
    # only speaks 3.1.1, answers a 5.0 CONNECT with a 3.1.1 CONNACK
    LegacyOnly
  end

  property behaviour : Behaviour = Behaviour::Modern

  getter transports = [] of FakeTransport

  # The protocol version of every CONNECT we were sent, in order
  getter attempts = [] of MQTT::Version

  def transport : FakeTransport
    @transports.last? || build_transport
  end

  def build_transport : FakeTransport
    transport = FakeTransport.new
    transport.on_send { |packet| respond(transport, packet) }
    @transports << transport
    transport
  end

  def factory : Proc(MQTT::Transport)
    -> { build_transport.as(MQTT::Transport) }
  end

  private def respond(transport : FakeTransport, packet : MQTT::V5::Header) : Nil
    case packet.id
    when .connect?
      version = connect_version(packet)
      @attempts << version
      answer_connect(transport, version)
    when .subscribe?
      answer_subscribe(transport, packet)
    when .publish?
      # a 3.1.1 publish has no property section, so it needs the matching parser
      message_id = if behaviour.legacy_only?
                     reparse(packet, MQTT::V3::Publish).message_id
                   else
                     reparse(packet, MQTT::V5::Publish).message_id
                   end
      send_puback(transport, message_id) if packet.qos.broker_received?
    when .unsubscribe?
      message_id = if behaviour.legacy_only?
                     reparse(packet, MQTT::V3::Unsubscribe).message_id
                   else
                     reparse(packet, MQTT::V5::Unsubscribe).message_id
                   end
      send_unsuback(transport, message_id)
    when .pingreq?
      resp = MQTT::V5::EmptyPacket.new
      resp.id = MQTT::RequestType::Pingresp
      resp.packet_length = 0_u32
      transport.receive_packet(resp)
    end
  end

  # The version byte sits after the protocol name, which is length prefixed.
  # Read it directly rather than parsing, since which parser applies is
  # precisely what we are trying to work out
  private def connect_version(packet : MQTT::V5::Header) : MQTT::Version
    io = IO::Memory.new
    io.write_bytes(packet)
    io.rewind

    io.read_bytes(MQTT::Header)
    name_size = io.read_bytes(UInt16, IO::ByteFormat::BigEndian)
    io.skip(name_size)

    byte = io.read_byte
    raise "CONNECT carried no version byte" unless byte
    MQTT::Version.from_value(byte)
  end

  private def answer_connect(transport : FakeTransport, version : MQTT::Version) : Nil
    case behaviour
    in Behaviour::Modern
      version.v5? ? send_v5_connack(transport, MQTT::V5::ReasonCode::Success) : send_v3_connack(transport, 0_u8)
    in Behaviour::RefusesFive
      if version.v5?
        send_v5_connack(transport, MQTT::V5::ReasonCode::UnsupportedProtocolVersion)
        transport.close!
      else
        send_v3_connack(transport, 0_u8)
      end
    in Behaviour::LegacyOnly
      if version.v5?
        # a 3.1.1 broker has no idea what 5.0 is: return code 1 in a 3.1.1
        # CONNACK, then close. A 5.0 parser chokes on this, by design
        send_v3_connack(transport, 1_u8)
        transport.close!
      else
        send_v3_connack(transport, 0_u8)
      end
    end
  end

  private def send_v5_connack(transport : FakeTransport, reason : MQTT::V5::ReasonCode) : Nil
    ack = MQTT::V5::Connack.new
    ack.id = MQTT::RequestType::Connack
    ack.reason_code = reason
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
  end

  private def send_v3_connack(transport : FakeTransport, return_code : UInt8) : Nil
    ack = MQTT::V3::Connack.new
    ack.id = MQTT::RequestType::Connack
    ack.return_code = return_code
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
  end

  private def answer_subscribe(transport : FakeTransport, packet : MQTT::V5::Header) : Nil
    if behaviour.legacy_only?
      sub = reparse(packet, MQTT::V3::Subscribe)
      ack = MQTT::V3::Suback.new
      ack.id = MQTT::RequestType::Suback
      ack.message_id = sub.message_id
      ack.raw_return_codes = sub.topics.map(&.qos.to_u8)
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    else
      sub = reparse(packet, MQTT::V5::Subscribe)
      ack = MQTT::V5::Suback.new
      ack.id = MQTT::RequestType::Suback
      ack.message_id = sub.message_id
      ack.reason_codes = sub.topics.map { |topic| MQTT::V5::ReasonCode.from_value(topic.qos.to_u8) }
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    end
  end

  private def send_puback(transport : FakeTransport, message_id : UInt16) : Nil
    # a bare PUBACK is byte identical in both versions
    ack = MQTT::V5::Ack.new
    ack.id = MQTT::RequestType::Puback
    ack.message_id = message_id
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
  end

  private def send_unsuback(transport : FakeTransport, message_id : UInt16) : Nil
    if behaviour.legacy_only?
      ack = MQTT::V3::Ack.new
      ack.id = MQTT::RequestType::Unsuback
      ack.message_id = message_id
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    else
      ack = MQTT::V5::Unsuback.new
      ack.id = MQTT::RequestType::Unsuback
      ack.message_id = message_id
      ack.reason_codes = [MQTT::V5::ReasonCode::Success]
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    end
  end

  private def reparse(packet : MQTT::V5::Header, klass : T.class) : T forall T
    io = IO::Memory.new
    io.write_bytes(packet)
    io.rewind
    io.read_bytes(klass)
  end
end
