require "./header"

module MQTT
  module V3
    class Publish < BinData
      endian big

      custom header : Header = Header.new

      # The topic name to publish to
      MQTT.string topic
      uint16 :message_id, onlyif: ->{ header.qos? }

      # The data to be published
      bytes :payload, length: ->{
        len = header.packet_length - (topic.bytesize + 2)
        len -= 2 if header.qos?
        len
      }

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        size = payload.size.to_u32
        size += topic.bytesize + 2
        size += 2 if qos?
        size
      end

      def payload=(message)
        @payload = message.to_slice
      end
    end
  end # V3
end   # MQTT
