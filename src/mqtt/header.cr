require "bindata"
require "./base"
require "./variable_byte_integer"

module MQTT
  # Performs binary encoding and decoding of the fixed header.
  #
  # NOTE:: the fixed header is byte identical in 3.1.1 and 5.0, so both
  # protocol versions build their packets on this
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

      @packet_length = VariableByteInteger.decode(
        variable_length1, variable_length2, variable_length3, variable_length4
      )
    end

    def packet_length=(size : UInt32) : UInt32
      encoded = VariableByteInteger.encode(size)

      {% for i in (1..4) %}
        self.variable_length{{ i.id }} = encoded[{{ i - 1 }}]
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
    # Avoids serialising the header just to measure it.
    #
    # NOTE:: this counts continuation bits rather than deriving the size from
    # the decoded value. A remote host can send a non-minimal encoding, and the
    # framing has to follow what is actually on the wire
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
end
