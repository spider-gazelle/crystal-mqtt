require "bindata"
require "../../mqtt"

module MQTT
  module V3
    # Performs binary encoding and decoding of headers
    class Header < BinData
      endian big

      bit_field do
        enum_bits 4, id : RequestType = RequestType::Connect
        bool duplicate
        enum_bits 2, qos : QoS = QoS::FireAndForget
        bool retain
      end
      uint8 :variable_length1
      uint8 :variable_length2, onlyif: ->{ variable_length1 & 0x80 > 0 }
      uint8 :variable_length3, onlyif: ->{ variable_length2 & 0x80 > 0 }
      uint8 :variable_length4, onlyif: ->{ variable_length3 & 0x80 > 0 }

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

    # Class representing an MQTT Connect Packet
    class Connect < BinData
      endian big

      custom header : Header = Header.new

      # The name of the protocol
      MQTT.string name, default: Version::V311.connect_name

      # The version number of the protocol
      enum_field UInt8, version : Version = Version::V311

      bit_field do
        bool has_username
        bool has_password
        # Set to true to make the Will message retained
        bool will_retain
        # The QoS level to send the Will message as
        enum_bits 2, will_qos : QoS = QoS::FireAndForget
        # flag to indicate if the will topic will be set
        bool will_flag
        # Set to false to keep a persistent session with the server
        bool clean_start
        bits 1, :_reserved_
      end

      uint16 :keep_alive_seconds

      # The client identifier string
      # NOTE:: Max size == 23 bytes for V3.1 (empty is not allowed)
      # NOTE:: Max size == 32 bytes for V3.1.1 (also empty is allowed)
      MQTT.string client_id

      # The topic name to send the Will message to
      MQTT.string will_topic, onlyif: ->{ will_flag } do
        self.will_flag = true
      end

      # The payload of the Will message
      MQTT.string will_payload, onlyif: ->{ will_flag }
      MQTT.string username, onlyif: ->{ has_username } do
        self.has_username = true
      end
      MQTT.string password, onlyif: ->{ has_password } do
        self.has_password = true
      end

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      # Packet length, excluding header
      def packet_length : UInt32
        size = 4_u32 + (name.bytesize + 2) + (client_id.bytesize + 2)
        size += (will_topic.bytesize + 2) if will_flag
        size += (will_payload.bytesize + 2) if will_flag
        size += (username.bytesize + 2) if has_username
        size += (password.bytesize + 2) if has_password
        size
      end
    end

    # Class representing an MQTT Connect Acknowledgment Packet
    class Connack < BinData
      endian big

      custom header : Header = Header.new

      bit_field do
        bits 7, :_reserved_
        bool session_present
      end

      # The return code (defaults to 0 for connection accepted)
      uint8 :return_code

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        2_u32
      end

      REFUSAL_CODES = {
        1_u8 => "Connection refused: unacceptable protocol version",
        2_u8 => "Connection refused: client identifier rejected",
        3_u8 => "Connection refused: server unavailable",
        4_u8 => "Connection refused: bad user name or password",
        5_u8 => "Connection refused: not authorised",
      }

      def success?
        return_code == 0_u8
      end

      def success!
        return true if success?
        raise (REFUSAL_CODES[return_code]? || "Connection refused: error code #{return_code}")
      end
    end

    class Publish < BinData
      endian big

      custom header : Header = Header.new

      # The topic name to publish to
      MQTT.string topic
      uint16 :message_id, onlyif: ->{ header.qos? }

      # The data to be published
      bytes :payload, length: ->{
        len = header.packet_length - (topic.bytesize + 2)
        len -= 2 if header.qos?
        len
      }

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        size = payload.size.to_u32
        size += topic.bytesize + 2
        size += 2 if qos?
        size
      end

      def payload=(message)
        @payload = message.to_slice
      end
    end

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

    class Topic < BinData
      endian big

      MQTT.string filter
      enum_field UInt8, qos : QoS = QoS::FireAndForget

      def bytesize
        # Size of string + string length + qos
        filter.bytesize + 3
      end
    end

    class Subscribe < BinData
      endian big

      # NOTE:: `header.flag2` should be `true` (qos == BrokerReceived)

      custom header : Header = Header.new
      uint16 :message_id

      # Will continue reading data into the array until the
      #  sum of the topics array + 2 message_id bytes equals the total size
      variable_array topics : Topic, read_next: ->{
        packet_length < header.packet_length
      }

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        topics.sum(2_u32, &.bytesize)
      end

      def topics=(array : Enumerable(String))
        self.topics = array.map do |filter|
          topic = Topic.new
          topic.filter = filter
          topic
        end
        array
      end

      def topics=(hash : Enumerable({String, QoS}))
        self.topics = hash.map do |filter, qos|
          topic = Topic.new
          topic.filter = filter
          topic.qos = qos
          topic
        end
        hash
      end

      def topic=(filter)
        topic = Topic.new
        topic.filter = filter
        self.topics = [topic]
        filter
      end
    end

    alias Unsubscribe = Subscribe

    class Suback < BinData
      endian big

      custom header : Header = Header.new
      uint16 :message_id
      variable_array raw_return_codes : UInt8, read_next: ->{
        packet_length < header.packet_length
      }

      delegate :id, :id=, :duplicate, :duplicate=, to: @header
      delegate :qos, :qos?, :qos=, :retain, :retain=, to: @header

      def packet_length : UInt32
        2_u32 + raw_return_codes.size
      end

      def return_codes
        raw_return_codes.map { |code| QoS.from_value code }
      end

      def return_codes=(codes : Enumerable(QoS))
        self.raw_return_codes = codes.map(&.to_u8)
        codes
      end
    end

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
