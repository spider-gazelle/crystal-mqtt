require "./header"

module MQTT
  module V3
    # Class representing an MQTT Connect Packet
    class Connect < Header
      endian big

      # The name of the protocol
      MQTT.string name, default: Version::V311.connect_name

      # The version number of the protocol
      field version : Version = Version::V311

      bit_field do
        bool has_username
        bool has_password
        # Set to true to make the Will message retained
        bool will_retain
        # The QoS level to send the Will message as
        bits 2, will_qos : QoS = QoS::FireAndForget
        # flag to indicate if the will topic will be set
        bool will_flag
        # Set to false to keep a persistent session with the server
        # http://www.steves-internet-guide.com/mqtt-clean-sessions-example/
        bool clean_start
        bits 1, :_reserved_
      end

      field keep_alive_seconds : UInt16

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

      # Packet length, excluding header
      def calculate_length : UInt32
        size = 4_u32 + (name.bytesize + 2) + (client_id.bytesize + 2)
        size += (will_topic.bytesize + 2) if will_flag
        size += (will_payload.bytesize + 2) if will_flag
        size += (username.bytesize + 2) if has_username
        size += (password.bytesize + 2) if has_password
        size
      end
    end
  end # V3
end   # MQTT
