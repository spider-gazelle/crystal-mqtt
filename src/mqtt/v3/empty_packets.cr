require "./header"

module MQTT
  module V3
    class Pingreq < BinData
      endian big
      custom header : Header = Header.new

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        0_u32
      end
    end

    alias Pingresp = Pingreq
    alias Disconnect = Pingreq
  end # V3
end   # MQTT
