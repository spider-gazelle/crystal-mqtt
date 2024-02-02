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
      field variable_length2 : UInt8, onlyif: ->{ variable_length1 & 0x80 > 0 }
      field variable_length3 : UInt8, onlyif: ->{ variable_length2 & 0x80 > 0 }
      field variable_length4 : UInt8, onlyif: ->{ variable_length3 & 0x80 > 0 }

      # invalid packet size to indicate if cached
      @packet_length : UInt32 = 0xFFFFFFFF

      def packet_length : UInt32
        return @packet_length unless @packet_length == 0xFFFFFFFF
        len = variable_length1.to_u32 & 0x7F_u32
        len += ((variable_length2.to_u32 & 0x7F_u32) * 0x80_u32)
        len += ((variable_length3.to_u32 & 0x7F_u32) * 0x4000_u32)
        len += ((variable_length4.to_u32 & 0x7F_u32) * 0x200000_u32)
        len
        @packet_length = len
      end

      def packet_length=(size : UInt32) : UInt32
        body_length = @packet_length = size
        {% for i in (1..4) %}
          self.variable_length{{i.id}} = (body_length % 128_u32).to_u8
          body_length = body_length // 128_u32
          self.variable_length{{i.id}} |= 0x80_u8 if body_length > 0_u32
        {% end %}
        size
      end

      def qos?
        self.qos != QoS::FireAndForget
      end
    end
  end # V3
end   # MQTT
