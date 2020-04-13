require "./header"

module MQTT
  module V3
    class Suback < Header
      endian big

      uint16 :message_id
      variable_array raw_return_codes : UInt8, read_next: ->{
        calculate_length < packet_length
      }

      def calculate_length : UInt32
        2_u32 + raw_return_codes.size
      end

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
