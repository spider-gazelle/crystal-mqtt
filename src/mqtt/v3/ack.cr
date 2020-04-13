require "./header"

module MQTT
  module V3
    class Ack < Header
      endian big

      uint16 :message_id

      def calculate_length : UInt32
        2_u32
      end
    end

    # NOTE:: Pubrel qos should be set to BrokerReceived
    alias Puback = Ack
    alias Pubrel = Ack
    alias Pubrec = Ack
    alias Pubcomp = Ack
    alias Unsuback = Ack
  end # V3
end   # MQTT
