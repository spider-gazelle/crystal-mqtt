require "bindata"
require "../../mqtt"

module MQTT
  module V3
    # Performs binary encoding and decoding of headers
    class Header < BinData
      endian big

      bit_field do
        bits 4, id : RequestType = RequestType::Connect
        bool duplicate
        bits 2, qos : QoS = QoS::FireAndForget
        bool retain
      end
      field variable_length1 : UInt8
      field variable_length2 : UInt8, onlyif: -> { variable_length1 & 0x80 > 0 }
      field variable_length3 : UInt8, onlyif: -> { variable_length2 & 0x80 > 0 }
      field variable_length4 : UInt8, onlyif: -> { variable_length3 & 0x80 > 0 }

      # invalid packet size to indicate if cached
      UNCACHED = 0xFFFFFFFF_u32

      @packet_length : UInt32 = UNCACHED

      def packet_length : UInt32
        cached = @packet_length
        return cached unless cached == UNCACHED

        len = variable_length1.to_u32 & 0x7F_u32
        len += ((variable_length2.to_u32 & 0x7F_u32) * 0x80_u32)
        len += ((variable_length3.to_u32 & 0x7F_u32) * 0x4000_u32)
        len += ((variable_length4.to_u32 & 0x7F_u32) * 0x200000_u32)
        @packet_length = len
      end

      def packet_length=(size : UInt32) : UInt32
        if size > MQTT::MAX_REMAINING_LENGTH
          raise MQTT::PacketError.new("packet length #{size} exceeds the maximum encodable remaining length of #{MQTT::MAX_REMAINING_LENGTH}")
        end

        body_length = size
        {% for i in (1..4) %}
          self.variable_length{{ i.id }} = (body_length % 128_u32).to_u8
          body_length = body_length // 128_u32
          self.variable_length{{ i.id }} |= 0x80_u8 if body_length > 0_u32
        {% end %}

        # the setters above invalidate the cache, so prime it last
        @packet_length = size
      end

      # The variable length fields are the source of truth once written
      # directly, so any write has to invalidate the decoded length
      {% for i in (1..4) %}
        def variable_length{{ i.id }}=(value : UInt8) : UInt8
          @packet_length = UNCACHED
          previous_def(value)
        end
      {% end %}

      # Number of bytes the fixed header occupies on the wire.
      # Avoids serialising the header just to measure it
      def fixed_header_size : Int32
        return 2 unless variable_length1 & 0x80 > 0
        return 3 unless variable_length2 & 0x80 > 0
        return 4 unless variable_length3 & 0x80 > 0
        5
      end

      def qos?
        qos != QoS::FireAndForget
      end
    end
  end # V3
end   # MQTT
