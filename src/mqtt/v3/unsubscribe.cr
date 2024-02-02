require "./header"

module MQTT
  module V3
    class UnsubTopic < BinData
      endian big

      MQTT.string filter

      def bytesize
        # Size of string + string length
        filter.bytesize + 2
      end
    end

    class Unsubscribe < Header
      endian big

      # NOTE:: `qos` should be `BrokerReceived`

      field message_id : UInt16

      # Will continue reading data into the array until the
      #  sum of the topics array + 2 message_id bytes equals the total size
      field topics : Array(UnsubTopic), read_next: ->{
        calculate_length < packet_length
      }

      def calculate_length : UInt32
        @topics.sum(2_u32, &.bytesize)
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = UnsubTopic.new
          topic.filter = filter
          topic
        end
        array
      end

      def topic=(filter)
        topic = UnsubTopic.new
        topic.filter = filter
        self.topics = [topic]
        filter
      end

      def topics
        @topics.map(&.filter)
      end
    end
  end # V3
end   # MQTT
