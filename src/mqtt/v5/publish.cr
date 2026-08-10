require "./ack"

module MQTT
  module V5
    # MQTT-3.3. Same as 3.1.1 with a property section between the packet
    # identifier and the payload
    class Publish < Header
      endian big

      MQTT.string topic
      field message_id : UInt16, onlyif: -> { qos? }
      field properties : Properties = Properties.new

      # NOTE:: the arithmetic is signed on purpose. A malformed packet can
      # advertise a remaining length smaller than the fields preceding the
      # payload, which underflows `UInt32` into a ~4GB allocation
      field payload : Bytes, length: -> {
        overhead = topic.bytesize + 2 + properties.total_size
        overhead += 2 if qos?
        len = packet_length.to_i64 - overhead
        if len < 0
          raise MQTT::ProtocolError.new("publish packet length #{packet_length} is too small for its topic and properties")
        end
        len
      }

      ALLOWED_PROPERTIES = [
        PropertyId::PayloadFormatIndicator,
        PropertyId::MessageExpiryInterval,
        PropertyId::TopicAlias,
        PropertyId::ResponseTopic,
        PropertyId::CorrelationData,
        PropertyId::UserProperty,
        PropertyId::SubscriptionIdentifier,
        PropertyId::ContentType,
      ]

      def calculate_length : UInt32
        size = payload.size.to_u32
        size += topic.bytesize + 2
        size += 2 if qos?
        size + properties.total_size
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "PUBLISH")
        self
      end

      def payload=(message)
        if message.nil?
          @payload = Bytes.new(0)
        else
          @payload = message.to_slice
        end
      end

      # ---- typed properties ---------------------------------------------------

      # 0 for arbitrary bytes, 1 for UTF-8. MQTT-3.3.2.3.2
      def payload_format_indicator : UInt8?
        properties.byte(PropertyId::PayloadFormatIndicator)
      end

      def payload_format_indicator=(value : UInt8?)
        properties.set_byte(PropertyId::PayloadFormatIndicator, value)
      end

      def utf8_payload? : Bool
        payload_format_indicator == 1_u8
      end

      def message_expiry_interval : UInt32?
        properties.four_byte(PropertyId::MessageExpiryInterval)
      end

      def message_expiry_interval=(value : UInt32?)
        properties.set_four_byte(PropertyId::MessageExpiryInterval, value)
      end

      # Replaces the topic string for the rest of the connection. Zero is not a
      # valid alias, MQTT-3.3.2.3.4
      def topic_alias : UInt16?
        properties.two_byte(PropertyId::TopicAlias)
      end

      def topic_alias=(value : UInt16?)
        if value && value.zero?
          raise ArgumentError.new("a topic alias must be greater than zero")
        end
        properties.set_two_byte(PropertyId::TopicAlias, value)
      end

      def content_type : String?
        properties.string(PropertyId::ContentType)
      end

      def content_type=(value : String?)
        properties.set_string(PropertyId::ContentType, value)
      end

      # The request/response pattern, MQTT-3.3.2.3.5
      def response_topic : String?
        properties.string(PropertyId::ResponseTopic)
      end

      def response_topic=(value : String?)
        properties.set_string(PropertyId::ResponseTopic, value)
      end

      def correlation_data : Bytes?
        properties.binary(PropertyId::CorrelationData)
      end

      def correlation_data=(value : Bytes?)
        properties.set_binary(PropertyId::CorrelationData, value)
      end

      # Which subscriptions caused the broker to send us this message.
      # Repeatable, because one message can match several subscriptions
      def subscription_identifiers : Array(UInt32)
        properties.variable_bytes(PropertyId::SubscriptionIdentifier)
      end

      def add_subscription_identifier(value : UInt32) : Nil
        properties.add_variable_byte(PropertyId::SubscriptionIdentifier, value)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end
    end
  end # V5
end   # MQTT
