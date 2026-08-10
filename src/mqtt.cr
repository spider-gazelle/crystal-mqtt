require "log"
require "random"

module MQTT
  Log = ::Log.for(self)

  # Default port number for unencrypted connections
  DEFAULT_PORT = 1883

  # Default port number for TLS/SSL encrypted connections
  DEFAULT_SSL_PORT = 8883

  macro string(name, onlyif = nil, default = nil, &block)
    field {{ name.id }}_size : UInt16, value: ->{ {{ name.id }}.bytesize }, onlyif: {{ onlyif }}
    field {{ name.id }} : String = {{ default }}, length: ->{ {{ name.id }}_size }, onlyif: {{ onlyif }}

    {% if block %}
      def {{ name.id }}=(str : String)
        {{ block.body }}
        previous_def(str)
      end
    {% end %}
  end

  enum Version : UInt8
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
        raise ProtocolError.new("unknown version #{self}")
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
    # 5.0 only, enhanced authentication
    Auth

    def requires_qos?
      self.in?({
        RequestType::Pubrel,
        RequestType::Subscribe,
        RequestType::Unsubscribe,
      })
    end
  end

  # https://makerdemy.com/what-is-quality-of-service-in-mqtt/
  enum QoS : UInt8
    FireAndForget       = 0
    BrokerReceived
    SubscribersReceived
  end

  def self.peek_type(io : IO)
    peek = io.peek
    raise ProtocolError.new("no data available to determine packet type") if peek.nil? || peek.empty?

    value = peek[0] >> 4
    RequestType.from_value?(value) || raise ProtocolError.new("reserved packet type #{value} is not valid")
  end

  def self.generate_client_id(prefix = "crystal")
    "#{prefix}#{Random::Secure.hex(8)}"
  end

  # A monotonic clock reading, used for idle detection in the keep alive.
  # `Time.monotonic` is deprecated from Crystal 1.21, but switching outright
  # would drop support for every earlier version
  {% if compare_versions(Crystal::VERSION, "1.21.0") >= 0 %}
    alias Monotonic = Time::Instant

    def self.monotonic : Monotonic
      Time.instant
    end
  {% else %}
    alias Monotonic = Time::Span

    def self.monotonic : Monotonic
      Time.monotonic
    end
  {% end %}

  # Super-class for other MQTT related exceptions.
  # Every error raised by this shard is a subclass of this, so
  # `rescue MQTT::Error` is sufficient to catch them all.
  class Error < ::Exception
  end

  # A ProtocolException will be raised if there is a
  # problem with data received from a remote host
  class ProtocolError < ::MQTT::Error
  end

  # A NotConnectedException will be raised when trying to
  # perform a function but no connection has been
  # established
  class NotConnectedError < ::MQTT::Error
  end

  # Raised when a broker fails to respond to a request within the
  # configured timeout. See `MQTT::V3::Client#timeout`
  class TimeoutError < ::MQTT::Error
  end

  # Raised when a packet cannot be encoded or decoded, for example a
  # remaining-length that exceeds what the variable length header supports
  class PacketError < ::MQTT::ProtocolError
  end

  # Raised when a broker refuses a connection, see `MQTT::V3::Connack#success!`
  class ConnectError < ::MQTT::Error
    getter return_code : UInt8

    def initialize(@return_code : UInt8, message : String)
      super(message)
    end
  end

  # Raised when a subscription could not be established
  class SubscriptionError < ::MQTT::Error
  end

  # The largest remaining-length the 4 byte variable length header can encode
  MAX_REMAINING_LENGTH = 268_435_455_u32

  # Default limit on the size of a single packet we're willing to buffer from a
  # remote host. Without a limit a hostile or faulty broker can advertise a
  # `MAX_REMAINING_LENGTH` sized packet and force us to buffer 256MB of data.
  DEFAULT_MAX_PACKET_SIZE = 8_u32 * 1024 * 1024
end

require "./mqtt/v3/*"
require "./mqtt/v5/*"
require "./mqtt/client"
