require "./header"

module MQTT
  module V3
    class SubTopic < BinData
      endian big

      MQTT.string filter
      field qos : QoS = QoS::FireAndForget

      def bytesize
        # Size of string + string length + qos
        filter.bytesize + 3
      end
    end

    class Subscribe < Header
      endian big

      # NOTE:: `qos` should be `BrokerReceived`

      field message_id : UInt16

      # Will continue reading data into the array until the
      #  sum of the topics array + 2 message_id bytes equals the total size
      field topics : Array(SubTopic), read_next: ->{
        calculate_length < packet_length
      }

      def calculate_length : UInt32
        topics.sum(2_u32, &.bytesize)
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = SubTopic.new
          topic.filter = filter
          topic
        end
        array
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

      def topic=(filter)
        topic = SubTopic.new
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
  end # V3
end   # MQTT
