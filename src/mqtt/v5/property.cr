require "bindata"
require "../base"
require "../variable_byte_integer"

module MQTT
  module V5
    # The seven data types a property value can take. Which one applies is
    # decided entirely by the identifier, there is nothing in the stream that
    # says so
    enum PropertyType
      Byte
      TwoByte
      FourByte
      VariableByte
      Utf8String
      BinaryData
      StringPair
    end

    # NOTE:: an identifier is strictly a variable byte integer, but every
    # identifier defined by the specification is <= 0x2A, so a single byte
    # covers all of them
    enum PropertyId : UInt8
      PayloadFormatIndicator           = 0x01
      MessageExpiryInterval            = 0x02
      ContentType                      = 0x03
      ResponseTopic                    = 0x08
      CorrelationData                  = 0x09
      SubscriptionIdentifier           = 0x0B
      SessionExpiryInterval            = 0x11
      AssignedClientIdentifier         = 0x12
      ServerKeepAlive                  = 0x13
      AuthenticationMethod             = 0x15
      AuthenticationData               = 0x16
      RequestProblemInformation        = 0x17
      WillDelayInterval                = 0x18
      RequestResponseInformation       = 0x19
      ResponseInformation              = 0x1A
      ServerReference                  = 0x1C
      ReasonString                     = 0x1F
      ReceiveMaximum                   = 0x21
      TopicAliasMaximum                = 0x22
      TopicAlias                       = 0x23
      MaximumQoS                       = 0x24
      RetainAvailable                  = 0x25
      UserProperty                     = 0x26
      MaximumPacketSize                = 0x27
      WildcardSubscriptionAvailable    = 0x28
      SubscriptionIdentifiersAvailable = 0x29
      SharedSubscriptionAvailable      = 0x2A

      def data_type : PropertyType
        case self
        in .payload_format_indicator?, .request_problem_information?, .request_response_information?,
           .maximum_qo_s?, .retain_available?, .wildcard_subscription_available?,
           .subscription_identifiers_available?, .shared_subscription_available?
          PropertyType::Byte
        in .server_keep_alive?, .receive_maximum?, .topic_alias_maximum?, .topic_alias?
          PropertyType::TwoByte
        in .message_expiry_interval?, .session_expiry_interval?, .will_delay_interval?,
           .maximum_packet_size?
          PropertyType::FourByte
        in .subscription_identifier?
          PropertyType::VariableByte
        in .content_type?, .response_topic?, .assigned_client_identifier?, .authentication_method?,
           .response_information?, .server_reference?, .reason_string?
          PropertyType::Utf8String
        in .correlation_data?, .authentication_data?
          PropertyType::BinaryData
        in .user_property?
          PropertyType::StringPair
        end
      end

      # May the property appear more than once in a single packet?
      def repeatable? : Bool
        user_property? || subscription_identifier?
      end
    end

    # One property: an identifier followed by exactly one value.
    #
    # `onlyif` picks the value's encoding from the identifier that was just
    # read, which is what lets the whole property system stay declarative
    class Property < BinData
      endian big

      field identifier : PropertyId = PropertyId::UserProperty

      field byte_value : UInt8 = 0_u8, onlyif: -> { identifier.data_type.byte? }
      field two_byte_value : UInt16 = 0_u16, onlyif: -> { identifier.data_type.two_byte? }
      field four_byte_value : UInt32 = 0_u32, onlyif: -> { identifier.data_type.four_byte? }

      # variable byte integer, a continuation bit per byte
      field vbi1 : UInt8 = 0_u8, onlyif: -> { variable_byte? }
      field vbi2 : UInt8 = 0_u8, onlyif: -> { variable_byte? && vbi1 & 0x80 > 0 }
      field vbi3 : UInt8 = 0_u8, onlyif: -> { variable_byte? && vbi2 & 0x80 > 0 }
      field vbi4 : UInt8 = 0_u8, onlyif: -> { variable_byte? && vbi3 & 0x80 > 0 }

      # a string pair is simply two strings back to back, so the first one
      # shares its encoding with a plain UTF-8 string
      field key_size : UInt16, value: -> { key.bytesize }, onlyif: -> { string? }
      field key : String = "", length: -> { key_size }, onlyif: -> { string? }
      field value_size : UInt16, value: -> { value.bytesize }, onlyif: -> { pair? }
      field value : String = "", length: -> { value_size }, onlyif: -> { pair? }

      field data_size : UInt16, value: -> { data.size }, onlyif: -> { binary? }
      field data : Bytes = Bytes.new(0), length: -> { data_size }, onlyif: -> { binary? }

      def variable_byte? : Bool
        identifier.data_type.variable_byte?
      end

      def pair? : Bool
        identifier.data_type.string_pair?
      end

      def string? : Bool
        identifier.data_type.utf8_string? || pair?
      end

      def binary? : Bool
        identifier.data_type.binary_data?
      end

      # The variable byte integer value, decoded
      def variable_value : UInt32
        VariableByteInteger.decode(vbi1, vbi2, vbi3, vbi4)
      end

      def variable_value=(value : UInt32) : UInt32
        encoded = VariableByteInteger.encode(value)
        self.vbi1 = encoded[0]
        self.vbi2 = encoded[1]
        self.vbi3 = encoded[2]
        self.vbi4 = encoded[3]
        value
      end

      # Bytes this property occupies on the wire
      def bytesize : Int32
        size = 1
        case identifier.data_type
        in .byte?          then size + 1
        in .two_byte?      then size + 2
        in .four_byte?     then size + 4
        in .variable_byte? then size + vbi_size
        in .utf8_string?   then size + 2 + key.bytesize
        in .string_pair?   then size + 2 + key.bytesize + 2 + value.bytesize
        in .binary_data?   then size + 2 + data.size
        end
      end

      private def vbi_size : Int32
        return 1 unless vbi1 & 0x80 > 0
        return 2 unless vbi2 & 0x80 > 0
        return 3 unless vbi3 & 0x80 > 0
        4
      end
    end

    # The property section carried by nearly every 5.0 packet: a variable byte
    # integer length, then properties until that many bytes are consumed
    class Properties < BinData
      endian big

      field len1 : UInt8, value: -> { encoded_length[0] }
      field len2 : UInt8, value: -> { encoded_length[1] }, onlyif: -> { len1 & 0x80 > 0 }
      field len3 : UInt8, value: -> { encoded_length[2] }, onlyif: -> { len2 & 0x80 > 0 }
      field len4 : UInt8, value: -> { encoded_length[3] }, onlyif: -> { len3 & 0x80 > 0 }

      field properties : Array(Property) = [] of Property, read_next: -> {
        properties.sum(0, &.bytesize) < declared_length
      }

      # Length the wire said the section would be
      def declared_length : Int32
        VariableByteInteger.decode(len1, len2, len3, len4).to_i32
      end

      # Length the properties we hold actually occupy
      def content_length : Int32
        properties.sum(0, &.bytesize)
      end

      # Total size on the wire, including the length prefix
      def total_size : Int32
        content = content_length
        VariableByteInteger.byte_size(content.to_u32) + content
      end

      def empty? : Bool
        properties.empty?
      end

      protected def encoded_length : StaticArray(UInt8, 4)
        VariableByteInteger.encode(content_length.to_u32)
      end

      # ---- typed access ------------------------------------------------------

      def find(id : PropertyId) : Property?
        properties.find { |property| property.identifier == id }
      end

      def all(id : PropertyId) : Array(Property)
        properties.select { |property| property.identifier == id }
      end

      def byte(id : PropertyId) : UInt8?
        find(id).try &.byte_value
      end

      def two_byte(id : PropertyId) : UInt16?
        find(id).try &.two_byte_value
      end

      def four_byte(id : PropertyId) : UInt32?
        find(id).try &.four_byte_value
      end

      def variable_byte(id : PropertyId) : UInt32?
        find(id).try &.variable_value
      end

      def string(id : PropertyId) : String?
        find(id).try &.key
      end

      def binary(id : PropertyId) : Bytes?
        find(id).try &.data
      end

      def pairs(id : PropertyId) : Array(Tuple(String, String))
        all(id).map { |property| {property.key, property.value} }
      end

      def variable_bytes(id : PropertyId) : Array(UInt32)
        all(id).map(&.variable_value)
      end

      # Setting a property replaces any existing one, and `nil` removes it
      {% for kind, config in {
                               byte:          {UInt8, "byte_value"},
                               two_byte:      {UInt16, "two_byte_value"},
                               four_byte:     {UInt32, "four_byte_value"},
                               variable_byte: {UInt32, "variable_value"},
                               string:        {String, "key"},
                               binary:        {Bytes, "data"},
                             } %}
        def set_{{ kind.id }}(id : PropertyId, value : {{ config[0].id }}?) : Nil
          delete(id)
          return if value.nil?

          property = Property.new
          property.identifier = id
          property.{{ config[1].id }} = value
          properties << property
        end
      {% end %}

      # Repeatable, so these append rather than replace
      def add_pair(id : PropertyId, key : String, value : String) : Nil
        property = Property.new
        property.identifier = id
        property.key = key
        property.value = value
        properties << property
      end

      def add_variable_byte(id : PropertyId, value : UInt32) : Nil
        property = Property.new
        property.identifier = id
        property.variable_value = value
        properties << property
      end

      def delete(id : PropertyId) : Nil
        properties.reject! { |property| property.identifier == id }
      end

      # ---- validation --------------------------------------------------------

      # Checks the section against the properties legal for a given packet.
      # A property that may only appear once, appearing twice, is a protocol
      # error rather than something to silently take the last of
      def validate!(allowed : Enumerable(PropertyId), packet : String) : self
        seen = Set(PropertyId).new

        properties.each do |property|
          id = property.identifier

          unless allowed.includes?(id)
            raise ProtocolError.new("#{id} is not a valid property for #{packet}")
          end

          next if id.repeatable?
          unless seen.add?(id)
            raise ProtocolError.new("#{id} may only appear once in #{packet}")
          end
        end

        self
      end
    end
  end # V5
end   # MQTT
