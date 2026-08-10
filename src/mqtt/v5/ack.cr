# NOTE:: `property` pulls in the root module first, which is what loads
# `MQTT::Header` cleanly. Requiring `../header` before it re-enters the root
# module while it is still loading, and the V3 alias then resolves to nothing
require "./property"
require "./reason_code"
require "../header"

module MQTT
  module V5
    alias Header = ::MQTT::Header

    # PUBACK, PUBREC, PUBREL and PUBCOMP.
    #
    # NOTE:: MQTT-3.4.2.1, the reason code and the property section are both
    # omitted when the reason is success and there are no properties. Nothing in
    # the packet says so — the remaining length is what tells you, which is
    # exactly what `onlyif` needs
    class Ack < Header
      endian big
      include ReasonCoded

      field message_id : UInt16
      field raw_reason_code : UInt8 = 0_u8, onlyif: -> { packet_length > 2 }
      field properties : Properties = Properties.new, onlyif: -> { packet_length > 3 }

      ALLOWED_PROPERTIES = [PropertyId::ReasonString, PropertyId::UserProperty]

      def calculate_length : UInt32
        # MQTT-3.4.2.1 allows omitting the reason code only when it is exactly
        # 0x00. Codes like NoMatchingSubscribers are successes but still have
        # to be sent, so this cannot key off `success?`
        return 2_u32 if raw_reason_code.zero? && properties.empty?
        return 3_u32 if properties.empty?
        3_u32 + properties.total_size
      end

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, id.to_s)
        self
      end

      def reason_string : String?
        properties.string(PropertyId::ReasonString)
      end

      def reason_string=(value : String?)
        properties.set_string(PropertyId::ReasonString, value)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end
    end

    alias Puback = Ack
    alias Pubrec = Ack
    alias Pubrel = Ack
    alias Pubcomp = Ack

    # DISCONNECT and AUTH have the same shape, minus the packet identifier.
    # A 5.0 broker may send DISCONNECT with a reason before closing, which
    # 3.1.1 had no way to express
    class Reasoned < Header
      endian big
      include ReasonCoded

      field raw_reason_code : UInt8 = 0_u8, onlyif: -> { packet_length > 0 }
      field properties : Properties = Properties.new, onlyif: -> { packet_length > 1 }

      def calculate_length : UInt32
        # as above, only a literal 0x00 may be omitted
        return 0_u32 if raw_reason_code.zero? && properties.empty?
        return 1_u32 if properties.empty?
        1_u32 + properties.total_size
      end

      def reason_string : String?
        properties.string(PropertyId::ReasonString)
      end

      def reason_string=(value : String?)
        properties.set_string(PropertyId::ReasonString, value)
      end

      def user_properties : Array(Tuple(String, String))
        properties.pairs(PropertyId::UserProperty)
      end

      def add_user_property(key : String, value : String) : Nil
        properties.add_pair(PropertyId::UserProperty, key, value)
      end
    end

    class Disconnect < Reasoned
      endian big

      ALLOWED_PROPERTIES = [
        PropertyId::SessionExpiryInterval,
        PropertyId::ReasonString,
        PropertyId::UserProperty,
        PropertyId::ServerReference,
      ]

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "DISCONNECT")
        self
      end

      def session_expiry_interval : UInt32?
        properties.four_byte(PropertyId::SessionExpiryInterval)
      end

      def session_expiry_interval=(value : UInt32?)
        properties.set_four_byte(PropertyId::SessionExpiryInterval, value)
      end

      # Where the broker is redirecting us to
      def server_reference : String?
        properties.string(PropertyId::ServerReference)
      end

      def server_reference=(value : String?)
        properties.set_string(PropertyId::ServerReference, value)
      end
    end

    # Enhanced authentication, MQTT-3.15
    class Auth < Reasoned
      endian big

      ALLOWED_PROPERTIES = [
        PropertyId::AuthenticationMethod,
        PropertyId::AuthenticationData,
        PropertyId::ReasonString,
        PropertyId::UserProperty,
      ]

      def validate! : self
        properties.validate!(ALLOWED_PROPERTIES, "AUTH")
        self
      end

      def authentication_method : String?
        properties.string(PropertyId::AuthenticationMethod)
      end

      def authentication_method=(value : String?)
        properties.set_string(PropertyId::AuthenticationMethod, value)
      end

      def authentication_data : Bytes?
        properties.binary(PropertyId::AuthenticationData)
      end

      def authentication_data=(value : Bytes?)
        properties.set_binary(PropertyId::AuthenticationData, value)
      end
    end

    # PINGREQ and PINGRESP are unchanged from 3.1.1
    class EmptyPacket < Header
      def calculate_length : UInt32
        0_u32
      end
    end

    alias Pingreq = EmptyPacket
    alias Pingresp = EmptyPacket
  end # V5
end   # MQTT
