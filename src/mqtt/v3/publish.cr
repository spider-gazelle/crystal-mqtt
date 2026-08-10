require "./header"

module MQTT
  module V3
    class Publish < Header
      endian big

      # The topic name to publish to
      MQTT.string topic
      field message_id : UInt16, onlyif: -> { qos? }

      # The data to be published
      #
      # NOTE:: the arithmetic here has to be signed. A malformed packet can
      # advertise a remaining-length smaller than the fields that precede the
      # payload, which underflows `UInt32` into a ~4GB allocation
      field payload : Bytes, length: -> {
        overhead = topic.bytesize + 2
        overhead += 2 if qos?
        len = packet_length.to_i64 - overhead
        if len < 0
          raise MQTT::ProtocolError.new("publish packet length #{packet_length} is too small for a #{topic.bytesize} byte topic")
        end
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
