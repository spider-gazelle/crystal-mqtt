require "./fake_transport"

# A scriptable broker sitting on top of `FakeTransport`.
#
# Responds to the packets a client sends the way a conforming broker would,
# with hooks for the misbehaviour we need to test against (silence, rejected
# subscriptions, wrong return code counts, dropped connections).
class FakeBroker
  # Every transport handed out, oldest first. A reconnecting client gets a
  # fresh one per attempt, just like a real socket
  getter transports = [] of FakeTransport

  # Packet types the broker should simply not respond to
  property silent : Array(MQTT::RequestType) = [] of MQTT::RequestType

  # Return codes to answer the next SUBACK with, overriding the requested QoS
  property suback_codes : Array(UInt8)? = nil

  # Return code for the next CONNACK
  property connack_return_code : UInt8 = 0_u8

  # Whether the next CONNACK reports a resumed session
  property? session_present : Bool = false

  # Raised by the next transport build, to simulate a broker that is still down
  property refuse_connections : Int32 = 0

  # Retained messages, exactly one per topic, surviving across connections the
  # way a real broker holds them
  getter retained = {} of String => Tuple(Bytes, MQTT::QoS)

  def transport : FakeTransport
    @transports.last? || build_transport
  end

  def build_transport : FakeTransport
    if @refuse_connections > 0
      @refuse_connections -= 1
      raise IO::Error.new("connection refused")
    end

    transport = FakeTransport.new
    transport.on_send { |packet| respond(transport, packet) }
    @transports << transport
    transport
  end

  # A client bound to a single transport, with no reconnection
  def client(**options) : MQTT::V3::Client
    MQTT::V3::Client.new(build_transport, **options)
  end

  # A client that builds a new transport whenever the connection drops
  def reconnecting_client(**options) : MQTT::V3::Client
    MQTT::V3::Client.new(**options) { build_transport }
  end

  # Simulates the connection dropping underneath the client
  def drop! : Nil
    transport.fail!(IO::Error.new("connection reset by peer"))
  end

  # Pushes a publish out to the client
  def publish(topic : String, payload : String = "", qos : MQTT::QoS = MQTT::QoS::FireAndForget, message_id : UInt16 = 1_u16, retain : Bool = false) : Nil
    packet = MQTT::V3::Publish.new
    packet.id = MQTT::RequestType::Publish
    packet.qos = qos
    packet.topic = topic
    packet.payload = payload
    packet.retain = retain
    packet.message_id = message_id unless qos.fire_and_forget?
    packet.packet_length = packet.calculate_length
    transport.receive_packet(packet)
  end

  # Delivers whatever we have retained for a filter, the way a broker does
  # immediately after granting a subscription
  private def deliver_retained(transport : FakeTransport, filter : String) : Nil
    retained.each do |topic, (payload, qos)|
      next unless MQTT.topic_matches?(filter, topic)

      packet = MQTT::V3::Publish.new
      packet.id = MQTT::RequestType::Publish
      packet.qos = MQTT::QoS::FireAndForget
      packet.topic = topic
      packet.payload = payload
      # MQTT-3.3.1-8, a message sent because of a new subscription is flagged
      packet.retain = true
      packet.packet_length = packet.calculate_length
      transport.receive_packet(packet)
    end
  end

  # MQTT-3.3.1-5..7, a retained publish replaces what we hold for the topic and
  # a zero length payload clears it
  private def store_retained(pub : MQTT::V3::Publish) : Nil
    return unless pub.retain

    if pub.payload.empty?
      retained.delete(pub.topic)
    else
      retained[pub.topic] = {pub.payload.dup, pub.qos}
    end
  end

  # Sends the PUBREL that completes an inbound QoS 2 delivery
  def release(message_id : UInt16) : Nil
    send_ack(transport, MQTT::RequestType::Pubrel, message_id, MQTT::QoS::BrokerReceived)
  end

  # Every packet type written across every transport this broker handed out
  def all_sent_types : Array(MQTT::RequestType)
    @transports.flat_map(&.sent_types)
  end

  def all_sent_packets(klass : T.class, type : MQTT::RequestType) : Array(T) forall T
    @transports.flat_map(&.sent_packets(klass, type))
  end

  private def respond(transport : FakeTransport, packet : MQTT::V3::Header) : Nil
    return if silent.includes?(packet.id)

    case packet.id
    when .connect?
      ack = MQTT::V3::Connack.new
      ack.id = MQTT::RequestType::Connack
      ack.return_code = connack_return_code
      ack.session_present = session_present?
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    when .subscribe?
      sub = reparse(packet, MQTT::V3::Subscribe)
      ack = MQTT::V3::Suback.new
      ack.id = MQTT::RequestType::Suback
      ack.message_id = sub.message_id
      ack.raw_return_codes = suback_codes || sub.topics.map(&.qos.to_u8)
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)

      # retained messages follow the SUBACK
      sub.topics.each { |topic| deliver_retained(transport, topic.filter) }
    when .unsubscribe?
      sub = reparse(packet, MQTT::V3::Unsubscribe)
      send_ack(transport, MQTT::RequestType::Unsuback, sub.message_id)
    when .publish?
      pub = reparse(packet, MQTT::V3::Publish)
      store_retained(pub)
      case pub.qos
      when .broker_received?
        send_ack(transport, MQTT::RequestType::Puback, pub.message_id)
      when .subscribers_received?
        send_ack(transport, MQTT::RequestType::Pubrec, pub.message_id)
      end
    when .pubrec?
      # the client is acknowledging an inbound QoS 2 message, release it
      rec = reparse(packet, MQTT::V3::Ack)
      send_ack(transport, MQTT::RequestType::Pubrel, rec.message_id, MQTT::QoS::BrokerReceived)
    when .pubrel?
      rel = reparse(packet, MQTT::V3::Ack)
      send_ack(transport, MQTT::RequestType::Pubcomp, rel.message_id)
    when .pingreq?
      resp = MQTT::V3::EmptyPacket.new
      resp.id = MQTT::RequestType::Pingresp
      resp.packet_length = 0_u32
      transport.receive_packet(resp)
    end
  end

  private def send_ack(transport : FakeTransport, type : MQTT::RequestType, message_id : UInt16, qos : MQTT::QoS = MQTT::QoS::FireAndForget) : Nil
    ack = MQTT::V3::Ack.new
    ack.id = type
    ack.qos = qos
    ack.message_id = message_id
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
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
