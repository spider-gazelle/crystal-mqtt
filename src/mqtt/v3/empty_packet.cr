require "./header"

module MQTT
  module V3
    class EmptyPacket < Header
      def calculate_length : UInt32
        0_u32
      end
    end

    alias Pingreq = EmptyPacket
    alias Pingresp = EmptyPacket
    alias Disconnect = EmptyPacket
  end # V3
end   # MQTT
