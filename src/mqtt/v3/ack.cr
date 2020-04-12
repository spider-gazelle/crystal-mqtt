require "./header"

module MQTT
  module V3
    class Puback < BinData
      endian big

      custom header : Header = Header.new
      uint16 :message_id

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        2_u32
      end
    end

    # NOTE:: Pubrel `header.flag2` should be `true` (qos == BrokerReceived)
    alias Pubrel = Puback
    alias Pubrec = Puback
    alias Pubcomp = Puback
    alias Unsuback = Puback
  end # V3
end   # MQTT
