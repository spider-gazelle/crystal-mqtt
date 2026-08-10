require "./fake_transport"
require "../../src/mqtt/v5/client"

# A scriptable 5.0 broker.
#
# The live specs cover everything mosquitto can do. This exists for the parts
# it cannot: enhanced authentication needs a broker plugin, and reason code
# failure paths are hard to provoke on demand from a conforming broker.
class FakeV5Broker
  getter transports = [] of FakeTransport

  # How many AUTH challenges to issue before accepting. Zero means no enhanced
  # authentication at all
  property auth_rounds : Int32 = 0

  # Ignore AUTH entirely, to model a broker that never finishes the exchange
  property? silent_auth : Bool = false

  # Every authentication payload the client sent us, in order
  getter auth_exchanges = [] of Bytes?

  # Reason code for the next CONNACK
  property connack_reason : MQTT::V5::ReasonCode = MQTT::V5::ReasonCode::Success

  # Reason code for the next PUBACK / PUBREC
  property puback_reason : MQTT::V5::ReasonCode = MQTT::V5::ReasonCode::Success
  property pubrec_reason : MQTT::V5::ReasonCode = MQTT::V5::ReasonCode::Success

  # Reason codes for the next SUBACK, one per filter, or nil to grant the QoS
  property suback_reasons : Array(MQTT::V5::ReasonCode)? = nil

  property? session_present : Bool = false

  # Properties the CONNACK advertises
  property topic_alias_maximum : UInt16 = 10_u16
  property receive_maximum : UInt16 = 20_u16

  @auth_remaining = 0
  @reauthenticating = false

  def transport : FakeTransport
    @transports.last? || build_transport
  end

  def build_transport : FakeTransport
    transport = FakeTransport.new
    transport.on_send { |packet| respond(transport, packet) }
    @transports << transport
    transport
  end

  def client(**options) : MQTT::V5::Client
    MQTT::V5::Client.new(build_transport, **options)
  end

  def drop! : Nil
    transport.fail!(IO::Error.new("connection reset by peer"))
  end

  # Sends an unsolicited DISCONNECT, the way a 5.0 broker can
  def disconnect!(reason : MQTT::V5::ReasonCode, server_reference : String? = nil) : Nil
    packet = MQTT::V5::Disconnect.new
    packet.id = MQTT::RequestType::Disconnect
    packet.reason_code = reason
    packet.server_reference = server_reference
    packet.packet_length = packet.calculate_length
    transport.receive_packet(packet)
  end

  def publish(topic : String, payload : String = "", qos : MQTT::QoS = MQTT::QoS::FireAndForget,
              message_id : UInt16 = 1_u16, retain : Bool = false, topic_alias : UInt16? = nil) : Nil
    packet = MQTT::V5::Publish.new
    packet.id = MQTT::RequestType::Publish
    packet.qos = qos
    packet.topic = topic
    packet.payload = payload
    packet.retain = retain
    packet.topic_alias = topic_alias if topic_alias
    packet.message_id = message_id unless qos.fire_and_forget?
    packet.packet_length = packet.calculate_length
    transport.receive_packet(packet)
  end

  def all_sent_types : Array(MQTT::RequestType)
    @transports.flat_map(&.sent_types)
  end

  def all_sent_packets(klass : T.class, type : MQTT::RequestType) : Array(T) forall T
    @transports.flat_map(&.sent_packets(klass, type))
  end

  private def respond(transport : FakeTransport, packet : MQTT::V5::Header) : Nil
    case packet.id
    when .connect?
      connect = reparse(packet, MQTT::V5::Connect)
      if auth_rounds > 0
        @auth_exchanges << connect.authentication_data
        @auth_remaining = auth_rounds
        challenge(transport, connect.authentication_method)
      else
        send_connack(transport)
      end
    when .auth?
      return if silent_auth?
      auth = reparse(packet, MQTT::V5::Auth)
      @auth_exchanges << auth.authentication_data

      case auth.reason_code
      when MQTT::V5::ReasonCode::ReAuthenticate
        @reauthenticating = true
        @auth_remaining = auth_rounds
        challenge(transport, auth.authentication_method)
      else
        @auth_remaining -= 1
        if @auth_remaining > 0
          challenge(transport, auth.authentication_method)
        elsif @reauthenticating
          # MQTT-4.12.1, a re-authentication finishes with AUTH carrying Success
          @reauthenticating = false
          finish_reauthentication(transport, auth.authentication_method)
        else
          # a first authentication is completed by the CONNACK
          send_connack(transport)
        end
      end
    else
      respond_to_message(transport, packet)
    end
  end

  private def respond_to_message(transport : FakeTransport, packet : MQTT::V5::Header) : Nil
    case packet.id
    when .subscribe?
      sub = reparse(packet, MQTT::V5::Subscribe)
      ack = MQTT::V5::Suback.new
      ack.id = MQTT::RequestType::Suback
      ack.message_id = sub.message_id
      ack.reason_codes = suback_reasons || sub.topics.map { |topic| granted(topic.qos) }
      ack.packet_length = ack.calculate_length
      transport.receive_packet(ack)
    when .unsubscribe?
      sub = reparse(packet, MQTT::V5::Unsubscribe)
      send_ack(transport, MQTT::RequestType::Unsuback, sub.message_id)
    when .publish?
      pub = reparse(packet, MQTT::V5::Publish)
      case pub.qos
      when .broker_received?
        send_ack(transport, MQTT::RequestType::Puback, pub.message_id, puback_reason)
      when .subscribers_received?
        send_ack(transport, MQTT::RequestType::Pubrec, pub.message_id, pubrec_reason)
      end
    when .pubrel?
      rel = reparse(packet, MQTT::V5::Ack)
      send_ack(transport, MQTT::RequestType::Pubcomp, rel.message_id)
    when .pubrec?
      rec = reparse(packet, MQTT::V5::Ack)
      send_ack(transport, MQTT::RequestType::Pubrel, rec.message_id, MQTT::V5::ReasonCode::Success, MQTT::QoS::BrokerReceived)
    when .pingreq?
      resp = MQTT::V5::EmptyPacket.new
      resp.id = MQTT::RequestType::Pingresp
      resp.packet_length = 0_u32
      transport.receive_packet(resp)
    end
  end

  private def challenge(transport : FakeTransport, method : String?) : Nil
    packet = MQTT::V5::Auth.new
    packet.id = MQTT::RequestType::Auth
    packet.reason_code = MQTT::V5::ReasonCode::ContinueAuthentication
    packet.authentication_method = method
    packet.authentication_data = "challenge-#{@auth_remaining}".to_slice
    packet.packet_length = packet.calculate_length
    transport.receive_packet(packet)
  end

  private def finish_reauthentication(transport : FakeTransport, method : String?) : Nil
    packet = MQTT::V5::Auth.new
    packet.id = MQTT::RequestType::Auth
    packet.reason_code = MQTT::V5::ReasonCode::Success
    packet.authentication_method = method
    packet.packet_length = packet.calculate_length
    transport.receive_packet(packet)
  end

  private def send_connack(transport : FakeTransport) : Nil
    ack = MQTT::V5::Connack.new
    ack.id = MQTT::RequestType::Connack
    ack.reason_code = connack_reason
    ack.session_present = session_present?
    ack.properties.set_two_byte(MQTT::V5::PropertyId::TopicAliasMaximum, topic_alias_maximum)
    ack.properties.set_two_byte(MQTT::V5::PropertyId::ReceiveMaximum, receive_maximum)
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
  end

  private def granted(qos : MQTT::QoS) : MQTT::V5::ReasonCode
    MQTT::V5::ReasonCode.from_value(qos.to_u8)
  end

  private def send_ack(transport : FakeTransport, type : MQTT::RequestType, message_id : UInt16,
                       reason : MQTT::V5::ReasonCode = MQTT::V5::ReasonCode::Success,
                       qos : MQTT::QoS = MQTT::QoS::FireAndForget) : Nil
    ack = MQTT::V5::Ack.new
    ack.id = type
    ack.qos = qos
    ack.message_id = message_id
    ack.reason_code = reason
    ack.packet_length = ack.calculate_length
    transport.receive_packet(ack)
  end

  private def reparse(packet : MQTT::V5::Header, klass : T.class) : T forall T
    io = IO::Memory.new
    io.write_bytes(packet)
    io.rewind
    io.read_bytes(klass)
  end
end
