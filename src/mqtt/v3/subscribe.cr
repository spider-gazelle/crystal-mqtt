require "./header"

module MQTT
  module V3
    class Topic < BinData
      endian big

      MQTT.string filter
      enum_field UInt8, qos : QoS = QoS::FireAndForget

      def bytesize
        # Size of string + string length + qos
        filter.bytesize + 3
      end
    end

    class Subscribe < BinData
      endian big

      # NOTE:: `qos` should be `BrokerReceived`

      custom header : Header = Header.new
      uint16 :message_id

      # Will continue reading data into the array until the
      #  sum of the topics array + 2 message_id bytes equals the total size
      variable_array topics : Topic, read_next: ->{
        packet_length < header.packet_length
      }

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        topics.sum(2_u32, &.bytesize)
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = Topic.new
          topic.filter = filter
          topic
        end
        array
      end

      def topics=(hash : Enumerable({String, QoS}))
        self.topics = hash.map do |filter, qos|
          topic = Topic.new
          topic.filter = filter
          topic.qos = qos
          topic
        end
        hash
      end

      def topic=(filter)
        topic = Topic.new
        topic.filter = filter
        self.topics = [topic]
        filter
      end

      def topic_hash
        hash = {} of String => QoS
        topics.each { |topic| hash[topic.filter] = topic.qos }
        hash
      end
    end

    alias Unsubscribe = Subscribe
  end # V3
end   # MQTT
