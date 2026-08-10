require "./ack"

module MQTT
  module V5
    # MQTT-3.1. Same shape as 3.1.1 with a property section after the keep
    # alive, and a second property section in front of the will topic
    class Connect < Header
      endian big

      MQTT.string name, default: "MQTT"

      field version : Version = Version::V5

      bit_field do
        bool has_username
        bool has_password
        bool will_retain
        bits 2, will_qos : QoS = QoS::FireAndForget
        bool will_flag
        bool clean_start
        bits 1, :_reserved_
      end

      field keep_alive_seconds : UInt16
      field properties : Properties = Properties.new

      MQTT.string client_id

      # the will has its own property section, and it precedes the will topic
      field will_properties : Properties = Properties.new, onlyif: -> { will_flag }

      MQTT.string will_topic, onlyif: -> { will_flag } do
        self.will_flag = true
      end

      # MQTT-3.1.3.3, the will payload is binary data rather than a string
      field will_payload_size : UInt16, value: -> { will_payload.size }, onlyif: -> { will_flag }
      field will_payload : Bytes = Bytes.new(0), length: -> { will_payload_size }, onlyif: -> { will_flag }

      MQTT.string username, onlyif: -> { has_username } do
        self.has_username = true
      end
      MQTT.string password, onlyif: -> { has_password } do
        self.has_password = true
      end

      ALLOWED_PROPERTIES = [
        PropertyId::SessionExpiryInterval,
        PropertyId::ReceiveMaximum,
        PropertyId::MaximumPacketSize,
        PropertyId::TopicAliasMaximum,
        PropertyId::RequestResponseInformation,
        PropertyId::RequestProblemInformation,
        PropertyId::UserProperty,
        PropertyId::AuthenticationMethod,
        PropertyId::AuthenticationData,
      ]

      ALLOWED_WILL_PROPERTIES = [
        PropertyId::WillDelayInterval,
        PropertyId::PayloadFormatIndicator,
        PropertyId::MessageExpiryInterval,
        PropertyId::ContentType,
        PropertyId::ResponseTopic,
        PropertyId::CorrelationData,
        PropertyId::UserProperty,
      ]

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "CONNECT")
        will_properties.validate!(ALLOWED_WILL_PROPERTIES, "CONNECT will") if will_flag
        self
      end

      def will_payload=(payload : String)
        self.will_payload = payload.to_slice
      end

      # Packet length, excluding the fixed header
      def calculate_length : UInt32
        size = 4_u32 + (name.bytesize + 2) + properties.total_size + (client_id.bytesize + 2)
        if will_flag
          size += will_properties.total_size
          size += (will_topic.bytesize + 2)
          size += (will_payload.size + 2)
        end
        size += (username.bytesize + 2) if has_username
        size += (password.bytesize + 2) if has_password
        size
      end

      # ---- typed properties ---------------------------------------------------

      # How long the broker keeps the session after we disconnect. 5.0 uses this
      # instead of 3.1.1's clean session flag alone
      def session_expiry_interval : UInt32?
        properties.four_byte(PropertyId::SessionExpiryInterval)
      end

      def session_expiry_interval=(value : UInt32?)
        properties.set_four_byte(PropertyId::SessionExpiryInterval, value)
      end

      # How many QoS > 0 messages we are willing to have in flight at once
      def receive_maximum : UInt16?
        properties.two_byte(PropertyId::ReceiveMaximum)
      end

      def receive_maximum=(value : UInt16?)
        properties.set_two_byte(PropertyId::ReceiveMaximum, value)
      end

      def maximum_packet_size : UInt32?
        properties.four_byte(PropertyId::MaximumPacketSize)
      end

      def maximum_packet_size=(value : UInt32?)
        properties.set_four_byte(PropertyId::MaximumPacketSize, value)
      end

      def topic_alias_maximum : UInt16?
        properties.two_byte(PropertyId::TopicAliasMaximum)
      end

      def topic_alias_maximum=(value : UInt16?)
        properties.set_two_byte(PropertyId::TopicAliasMaximum, value)
      end

      def authentication_method : String?
        properties.string(PropertyId::AuthenticationMethod)
      end

      def authentication_method=(value : String?)
        properties.set_string(PropertyId::AuthenticationMethod, value)
      end

      def authentication_data : Bytes?
        properties.binary(PropertyId::AuthenticationData)
      end

      def authentication_data=(value : Bytes?)
        properties.set_binary(PropertyId::AuthenticationData, value)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end

      # How long the broker waits before publishing the will
      def will_delay_interval : UInt32?
        will_properties.four_byte(PropertyId::WillDelayInterval)
      end

      def will_delay_interval=(value : UInt32?)
        will_properties.set_four_byte(PropertyId::WillDelayInterval, value)
      end
    end

    # MQTT-3.2
    class Connack < Header
      endian big
      include ReasonCoded

      bit_field do
        bits 7, :_reserved_
        bool session_present
      end

      field raw_reason_code : UInt8 = 0_u8
      field properties : Properties = Properties.new

      ALLOWED_PROPERTIES = [
        PropertyId::SessionExpiryInterval,
        PropertyId::ReceiveMaximum,
        PropertyId::MaximumQoS,
        PropertyId::RetainAvailable,
        PropertyId::MaximumPacketSize,
        PropertyId::AssignedClientIdentifier,
        PropertyId::TopicAliasMaximum,
        PropertyId::ReasonString,
        PropertyId::UserProperty,
        PropertyId::WildcardSubscriptionAvailable,
        PropertyId::SubscriptionIdentifiersAvailable,
        PropertyId::SharedSubscriptionAvailable,
        PropertyId::ServerKeepAlive,
        PropertyId::ResponseInformation,
        PropertyId::ServerReference,
        PropertyId::AuthenticationMethod,
        PropertyId::AuthenticationData,
      ]

      def calculate_length : UInt32
        2_u32 + properties.total_size
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "CONNACK")
        self
      end

      def success! : self
        return self if success?
        raise ConnectError.new(raw_reason_code, "Connection refused: #{reason_description}")
      end

      # ---- broker capabilities ------------------------------------------------

      # The identifier the broker picked for us, when we sent an empty one
      def assigned_client_identifier : String?
        properties.string(PropertyId::AssignedClientIdentifier)
      end

      # A broker may impose its own keep alive, overriding what we asked for
      def server_keep_alive : UInt16?
        properties.two_byte(PropertyId::ServerKeepAlive)
      end

      def session_expiry_interval : UInt32?
        properties.four_byte(PropertyId::SessionExpiryInterval)
      end

      # How many QoS > 0 messages the broker will accept in flight
      def receive_maximum : UInt16
        properties.two_byte(PropertyId::ReceiveMaximum) || 65_535_u16
      end

      def maximum_qos : QoS
        raw = properties.byte(PropertyId::MaximumQoS)
        raw ? QoS.from_value(raw) : QoS::SubscribersReceived
      end

      def maximum_packet_size : UInt32?
        properties.four_byte(PropertyId::MaximumPacketSize)
      end

      def topic_alias_maximum : UInt16
        properties.two_byte(PropertyId::TopicAliasMaximum) || 0_u16
      end

      # Defaults are "available" when the broker says nothing, MQTT-3.2.2.3
      {% for name, id in {
                           retain_available:                  "RetainAvailable",
                           wildcard_subscription_available:   "WildcardSubscriptionAvailable",
                           subscription_identifier_available: "SubscriptionIdentifiersAvailable",
                           shared_subscription_available:     "SharedSubscriptionAvailable",
                         } %}
        def {{ name.id }}? : Bool
          value = properties.byte(PropertyId::{{ id.id }})
          value.nil? || value != 0_u8
        end
      {% end %}

      def response_information : String?
        properties.string(PropertyId::ResponseInformation)
      end

      def server_reference : String?
        properties.string(PropertyId::ServerReference)
      end

      def reason_string : String?
        properties.string(PropertyId::ReasonString)
      end

      def authentication_method : String?
        properties.string(PropertyId::AuthenticationMethod)
      end

      def authentication_data : Bytes?
        properties.binary(PropertyId::AuthenticationData)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end
    end
  end # V5
end   # MQTT
