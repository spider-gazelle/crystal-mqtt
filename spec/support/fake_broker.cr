require "./fake_transport"

# A scriptable broker sitting on top of `FakeTransport`.
#
# Responds to the packets a client sends the way a conforming broker would,
# with hooks for the misbehaviour we need to test against (silence, rejected
# subscriptions, wrong return code counts).
class FakeBroker
  getter transport : FakeTransport

  # Packet types the broker should simply not respond to
  property silent : Array(MQTT::RequestType) = [] of MQTT::RequestType

  # Return codes to answer the next SUBACK with, overriding the requested QoS
  property suback_codes : Array(UInt8)? = nil

  # Return code for the next CONNACK
  property connack_return_code : UInt8 = 0_u8

  def initialize
    @transport = FakeTransport.new
    @transport.on_send { |packet| respond(packet) }
  end

  def client(**options) : MQTT::V3::Client
    MQTT::V3::Client.new(@transport, **options)
  end

  # Pushes a publish out to the client
  def publish(topic : String, payload : String = "", qos : MQTT::QoS = MQTT::QoS::FireAndForget, message_id : UInt16 = 1_u16) : Nil
    packet = MQTT::V3::Publish.new
    packet.id = MQTT::RequestType::Publish
    packet.qos = qos
    packet.topic = topic
    packet.payload = payload
    packet.message_id = message_id unless qos.fire_and_forget?
    packet.packet_length = packet.calculate_length
    @transport.receive_packet(packet)
  end

  # Sends the PUBREL that completes an inbound QoS 2 delivery
  def release(message_id : UInt16) : Nil
    send_ack(MQTT::RequestType::Pubrel, message_id, MQTT::QoS::BrokerReceived)
  end

  private def respond(packet : MQTT::V3::Header) : Nil
    return if silent.includes?(packet.id)

    case packet.id
    when .connect?
      ack = MQTT::V3::Connack.new
      ack.id = MQTT::RequestType::Connack
      ack.return_code = connack_return_code
      ack.packet_length = ack.calculate_length
      @transport.receive_packet(ack)
    when .subscribe?
      sub = reparse(packet, MQTT::V3::Subscribe)
      ack = MQTT::V3::Suback.new
      ack.id = MQTT::RequestType::Suback
      ack.message_id = sub.message_id
      ack.raw_return_codes = suback_codes || sub.topics.map(&.qos.to_u8)
      ack.packet_length = ack.calculate_length
      @transport.receive_packet(ack)
    when .unsubscribe?
      sub = reparse(packet, MQTT::V3::Unsubscribe)
      send_ack(MQTT::RequestType::Unsuback, sub.message_id)
    when .publish?
      pub = reparse(packet, MQTT::V3::Publish)
      case pub.qos
      when .broker_received?
        send_ack(MQTT::RequestType::Puback, pub.message_id)
      when .subscribers_received?
        send_ack(MQTT::RequestType::Pubrec, pub.message_id)
      end
    when .pubrec?
      # the client is acknowledging an inbound QoS 2 message, release it
      rec = reparse(packet, MQTT::V3::Ack)
      release(rec.message_id)
    when .pubrel?
      rel = reparse(packet, MQTT::V3::Ack)
      send_ack(MQTT::RequestType::Pubcomp, rel.message_id)
    when .pingreq?
      resp = MQTT::V3::EmptyPacket.new
      resp.id = MQTT::RequestType::Pingresp
      resp.packet_length = 0_u32
      @transport.receive_packet(resp)
    end
  end

  private def send_ack(type : MQTT::RequestType, message_id : UInt16, qos : MQTT::QoS = MQTT::QoS::FireAndForget) : Nil
    ack = MQTT::V3::Ack.new
    ack.id = type
    ack.qos = qos
    ack.message_id = message_id
    ack.packet_length = ack.calculate_length
    @transport.receive_packet(ack)
  end

  # The client hands us the object it wrote, round-trip it through bytes so we
  # only ever act on what actually went over the wire
  private def reparse(packet : MQTT::V3::Header, klass : T.class) : T forall T
    io = IO::Memory.new
    io.write_bytes(packet)
    io.rewind
    io.read_bytes(klass)
  end
end

# Runs a block with a connected client and its broker
def with_client(**options, &)
  broker = FakeBroker.new
  client = broker.client(**options)
  client.connect
  begin
    yield client, broker
  ensure
    client.disconnect(send_msg: false) rescue nil
  end
end
