require "log"

module MQTT
  Log = ::Log.for("mqtt")

  # Default port number for unencrypted connections
  DEFAULT_PORT = 1883

  # Default port number for TLS/SSL encrypted connections
  DEFAULT_SSL_PORT = 8883

  macro string(name, onlyif = nil, default = nil, &block)
    uint16 :{{name.id}}_size, value: ->{ {{name.id}}.bytesize }, onlyif: {{onlyif}}
    string :{{name.id}}, length: ->{ {{name.id}}_size }, onlyif: {{onlyif}}, default: {{default}}

    {% if block %}
      def {{name.id}}=(str : String)
        {{block.body}}
        previous_def(str)
      end
    {% end %}
  end

  enum Version
    V31  = 3
    V311
    V5

    def connect_name : String
      case self
      when Version::V31
        "MQIsdp"
      when Version::V311
        "MQTT"
      when Version::V5
        "MQTT"
      else
        raise "unknown version #{self}"
      end
    end
  end

  enum RequestType
    Connect     = 1
    Connack
    Publish
    Puback
    Pubrec
    Pubrel
    Pubcomp
    Subscribe
    Suback
    Unsubscribe
    Unsuback
    Pingreq
    Pingresp
    Disconnect

    def requires_qos?
      self.in?({
        RequestType::Pubrel,
        RequestType::Subscribe,
        RequestType::Unsubscribe,
      })
    end
  end

  # https://makerdemy.com/what-is-quality-of-service-in-mqtt/
  enum QoS
    FireAndForget       = 0
    BrokerReceived
    SubscribersReceived
  end

  def self.peek_type(io : IO)
    RequestType.from_value(io.peek[0] >> 4)
  end

  # Super-class for other MQTT related exceptions
  class Exception < ::Exception
  end

  # A ProtocolException will be raised if there is a
  # problem with data received from a remote host
  class ProtocolException < ::MQTT::Exception
  end

  # A NotConnectedException will be raised when trying to
  # perform a function but no connection has been
  # established
  class NotConnectedException < ::MQTT::Exception
  end

  # MQTT-SN
  module SN
    # Default port number for unencrypted connections
    DEFAULT_PORT = 1883

    # A ProtocolException will be raised if there is a
    # problem with data received from a remote host
    class ProtocolException < ::MQTT::Exception
    end
  end
end

require "./mqtt/v3/*"
