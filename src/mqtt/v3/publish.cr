require "./header"

module MQTT
  module V3
    class Publish < Header
      endian big

      # The topic name to publish to
      MQTT.string topic
      field message_id : UInt16, onlyif: ->{ qos? }

      # The data to be published
      field payload : Bytes, length: ->{
        len = packet_length - (topic.bytesize + 2)
        len -= 2 if qos?
        len
      }

      def calculate_length : UInt32
        size = payload.size.to_u32
        size += topic.bytesize + 2
        size += 2 if qos?
        size
      end

      def payload=(message)
        if message.nil?
          @payload = Bytes.new(0)
        else
          @payload = message.to_slice
        end
      end
    end
  end # V3
end   # MQTT
