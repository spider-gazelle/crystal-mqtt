require "./header"

module MQTT
  module V3
    class Suback < Header
      endian big

      field message_id : UInt16
      field raw_return_codes : Array(UInt8), read_next: -> {
        calculate_length < packet_length
      }

      def calculate_length : UInt32
        2_u32 + raw_return_codes.size
      end

      # A broker signals a rejected topic filter with 0x80, which is not a
      # valid `QoS` value
      FAILURE = 0x80_u8

      def failure?(code : UInt8) : Bool
        code == FAILURE
      end

      # NOTE:: raises if the broker rejected any of the topic filters,
      # use `raw_return_codes` when a rejection is possible
      def return_codes
        raw_return_codes.map { |code| QoS.from_value code }
      end

      def return_codes=(codes : Enumerable(QoS))
        self.raw_return_codes = codes.map(&.to_u8)
        codes
      end
    end
  end # V3
end   # MQTT
