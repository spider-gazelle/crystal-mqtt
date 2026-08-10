require "./ack"

module MQTT
  module V5
    # MQTT-3.8.3.1. Controls whether retained messages are delivered when a
    # subscription is made
    enum RetainHandling : UInt8
      # send retained messages when the subscription is established
      SendAlways = 0
      # only send them if the subscription did not already exist
      SendIfNew = 1
      # never send retained messages for this subscription
      Never = 2
    end

    # A topic filter plus the 5.0 subscription options byte
    class SubTopic < BinData
      endian big

      MQTT.string filter

      bit_field do
        bits 2, :_reserved_
        bits 2, retain_handling : RetainHandling = RetainHandling::SendAlways
        # keep the retain flag the publisher set, rather than clearing it
        bool retain_as_published
        # don't deliver our own publications back to us
        bool no_local
        bits 2, qos : QoS = QoS::FireAndForget
      end

      def bytesize
        # string + its length prefix + the options byte
        filter.bytesize + 3
      end
    end

    # MQTT-3.8
    class Subscribe < Header
      endian big

      field message_id : UInt16
      field properties : Properties = Properties.new

      field topics : Array(SubTopic) = [] of SubTopic, read_next: -> {
        consumed < packet_length
      }

      ALLOWED_PROPERTIES = [PropertyId::SubscriptionIdentifier, PropertyId::UserProperty]

      # bytes accounted for so far: identifier, properties, and the filters read
      private def consumed : UInt32
        2_u32 + properties.total_size + topics.sum(0, &.bytesize)
      end

      def calculate_length : UInt32
        consumed
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "SUBSCRIBE")
        self
      end

      # Tags every message delivered by this subscription, so a client can route
      # by identifier instead of re-matching the filter
      def subscription_identifier : UInt32?
        properties.variable_byte(PropertyId::SubscriptionIdentifier)
      end

      def subscription_identifier=(value : UInt32?)
        properties.set_variable_byte(PropertyId::SubscriptionIdentifier, value)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end

      def topics=(hash : Enumerable({String, QoS}))
        self.topics = hash.map do |filter, qos|
          topic = SubTopic.new
          topic.filter = filter
          topic.qos = qos
          topic
        end
        hash
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = SubTopic.new
          topic.filter = filter
          topic
        end
        array
      end

      def topic_hash
        hash = {} of String => QoS
        topics.each { |topic| hash[topic.filter] = topic.qos }
        hash
      end
    end

    # MQTT-3.9. One reason code per filter, in the order they were requested
    class Suback < Header
      endian big

      field message_id : UInt16
      field properties : Properties = Properties.new

      field raw_reason_codes : Array(UInt8) = [] of UInt8, read_next: -> {
        consumed < packet_length
      }

      ALLOWED_PROPERTIES = [PropertyId::ReasonString, PropertyId::UserProperty]

      private def consumed : UInt32
        2_u32 + properties.total_size + raw_reason_codes.size
      end

      def calculate_length : UInt32
        consumed
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "SUBACK")
        self
      end

      def reason_codes : Array(ReasonCode?)
        raw_reason_codes.map { |code| ReasonCode.from_value?(code) }
      end

      def reason_codes=(codes : Enumerable(ReasonCode))
        self.raw_reason_codes = codes.map(&.value)
        codes
      end

      def failure?(code : UInt8) : Bool
        code >= 0x80_u8
      end

      def reason_string : String?
        properties.string(PropertyId::ReasonString)
      end
    end

    # MQTT-3.10
    class UnsubTopic < BinData
      endian big

      MQTT.string filter

      def bytesize
        filter.bytesize + 2
      end
    end

    class Unsubscribe < Header
      endian big

      field message_id : UInt16
      field properties : Properties = Properties.new

      field topics : Array(UnsubTopic) = [] of UnsubTopic, read_next: -> {
        consumed < packet_length
      }

      # MQTT-3.10.2.1, only user properties are allowed here
      ALLOWED_PROPERTIES = [PropertyId::UserProperty]

      private def consumed : UInt32
        2_u32 + properties.total_size + @topics.sum(0, &.bytesize)
      end

      def calculate_length : UInt32
        consumed
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "UNSUBSCRIBE")
        self
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = UnsubTopic.new
          topic.filter = filter
          topic
        end
        array
      end

      def topics
        @topics.map(&.filter)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end
    end

    # MQTT-3.11
    class Unsuback < Header
      endian big

      field message_id : UInt16
      field properties : Properties = Properties.new

      field raw_reason_codes : Array(UInt8) = [] of UInt8, read_next: -> {
        consumed < packet_length
      }

      ALLOWED_PROPERTIES = [PropertyId::ReasonString, PropertyId::UserProperty]

      private def consumed : UInt32
        2_u32 + properties.total_size + raw_reason_codes.size
      end

      def calculate_length : UInt32
        consumed
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "UNSUBACK")
        self
      end

      def reason_codes : Array(ReasonCode?)
        raw_reason_codes.map { |code| ReasonCode.from_value?(code) }
      end

      def reason_codes=(codes : Enumerable(ReasonCode))
        self.raw_reason_codes = codes.map(&.value)
        codes
      end

      def failure?(code : UInt8) : Bool
        code >= 0x80_u8
      end
    end
  end # V5
end   # MQTT
