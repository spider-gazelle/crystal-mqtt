require "../mqtt"

module MQTT
  # MQTT encodes lengths as a variable byte integer: seven bits of payload per
  # byte, with the top bit signalling that another byte follows. Four bytes is
  # the maximum, giving `MQTT::MAX_REMAINING_LENGTH`.
  #
  # 3.1.1 only uses this for the remaining length in the fixed header. 5.0 also
  # uses it for the property section length and for subscription identifiers.
  module VariableByteInteger
    # Number of bytes the encoded form of *value* occupies
    def self.byte_size(value : UInt32) : Int32
      return 1 if value < 128
      return 2 if value < 16_384
      return 3 if value < 2_097_152
      4
    end

    # NOTE:: the caller passes every byte, including ones that were never read
    # from the wire. Those default to zero and contribute nothing
    def self.decode(byte1 : UInt8, byte2 : UInt8, byte3 : UInt8, byte4 : UInt8) : UInt32
      len = byte1.to_u32 & 0x7F_u32
      len += ((byte2.to_u32 & 0x7F_u32) * 0x80_u32)
      len += ((byte3.to_u32 & 0x7F_u32) * 0x4000_u32)
      len += ((byte4.to_u32 & 0x7F_u32) * 0x200000_u32)
      len
    end

    # Always returns four bytes, the unused trailing ones are zero.
    # `byte_size` says how many of them belong on the wire
    def self.encode(value : UInt32) : StaticArray(UInt8, 4)
      if value > MQTT::MAX_REMAINING_LENGTH
        raise MQTT::PacketError.new("length #{value} exceeds the maximum encodable variable byte integer of #{MQTT::MAX_REMAINING_LENGTH}")
      end

      bytes = StaticArray(UInt8, 4).new(0_u8)
      remaining = value
      4.times do |index|
        bytes[index] = (remaining % 128_u32).to_u8
        remaining = remaining // 128_u32
        bytes[index] |= 0x80_u8 if remaining > 0_u32
      end
      bytes
    end
  end
end
