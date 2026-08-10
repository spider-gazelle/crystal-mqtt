require "./v5/client"
require "./v3/client"

module MQTT
  # Connects with MQTT 5.0 and falls back to 3.1.1 when the broker will not
  # take it, exposing what the two protocols have in common.
  #
  # A broker that rejects the version **closes the connection**, so negotiation
  # needs a fresh transport for the retry. That is why this takes a factory
  # rather than a transport:
  #
  # ```
  # client = MQTT::Client.new { MQTT::Transport::TCP.new("test.mosquitto.org") }
  # client.connect
  # client.version # => MQTT::Version::V5
  # ```
  #
  # Version specific features stay on `MQTT::V5::Client`, reachable through
  # `#v5` once you know what you are talking to.
  class Client
    alias Callback = Proc(String, Bytes, Nil) | Proc(String, Bytes, Bool, Nil)

    # The version actually negotiated, nil until `#connect` has run
    getter version : Version? = nil

    # Whether the broker resumed an existing session
    getter? session_present : Bool = false

    def initialize(
      @timeout : Time::Span? = MQTT::ClientBase::DEFAULT_TIMEOUT,
      @max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
      @reconnect : MQTT::Reconnect = MQTT::Reconnect.new,
      &factory : -> Transport
    )
      @factory = factory
    end

    @client : (V3::Client | V5::Client)?

    # The negotiated client. Raises until `#connect` has succeeded
    def negotiated : V3::Client | V5::Client
      @client || raise NotConnectedError.new("connect has not been called")
    end

    # The 5.0 client, when 5.0 is what was negotiated
    def v5 : V5::Client?
      @client.as?(V5::Client)
    end

    # The 3.1.1 client, when that is what was negotiated
    def v3 : V3::Client?
      @client.as?(V3::Client)
    end

    # Tries 5.0, then 3.1.1. Once a version is settled the underlying client
    # keeps the factory, so reconnections use it directly and never re-probe
    def connect(
      username : String? = nil,
      password : String? = nil,
      keep_alive : Int32 = 60,
      client_id : String = MQTT.generate_client_id,
      clean_start : Bool = true,
      will_flag : Bool = false,
      will_qos : Int32 | QoS = 0,
      will_retain : Bool = false,
      will_topic : String? = nil,
      will_payload : (String | Bytes)? = nil,
      timeout : Time::Span? = @timeout,
      keep_alive_active : Bool = true,
    ) : Version
      modern = V5::Client.new(
        timeout: @timeout, max_packet_size: @max_packet_size, reconnect: @reconnect, &@factory
      )

      begin
        ack = modern.connect(
          username: username, password: password, keep_alive: keep_alive,
          client_id: client_id, clean_start: clean_start, will_flag: will_flag,
          will_qos: will_qos, will_retain: will_retain, will_topic: will_topic,
          will_payload: will_payload, timeout: timeout, keep_alive_active: keep_alive_active
        )
        ack.success!

        @client = modern
        @version = Version::V5
        @session_present = ack.session_present
        return Version::V5
      rescue error
        raise error unless downgrade?(error)

        Log.info { "broker did not accept MQTT 5.0 (#{error.message}), falling back to 3.1.1" }
        modern.disconnect(send_msg: false) rescue nil
      end

      legacy = V3::Client.new(
        timeout: @timeout, max_packet_size: @max_packet_size, reconnect: @reconnect, &@factory
      )
      ack = legacy.connect(
        username: username, password: password, keep_alive: keep_alive,
        client_id: client_id, clean_start: clean_start, will_flag: will_flag,
        will_qos: will_qos, will_retain: will_retain, will_topic: will_topic,
        will_payload: will_payload, timeout: timeout, keep_alive_active: keep_alive_active
      )
      ack.success!

      @client = legacy
      @version = Version::V311
      @session_present = ack.session_present
      Version::V311
    end

    # Does this failure mean "try an older protocol", rather than "give up"?
    #
    # A 5.0 broker answers with reason 0x84. A 3.1.1 broker has no idea what
    # 5.0 is and replies with a 3.1.1 CONNACK, which the 5.0 parser cannot
    # decode — so the connection simply dies. Both have to count, while a
    # transport that never connected at all must not
    private def downgrade?(error : ::Exception) : Bool
      case error
      when ConnectError
        # 0x84 in 5.0, return code 1 in 3.1.1, both "unacceptable version"
        error.return_code == 0x84_u8 || error.return_code == 1_u8
      when ProtocolError
        true
      when NotConnectedError
        # a socket that never opened is a connectivity problem, not a protocol
        # one, and its cause is the underlying failure
        error.cause.nil?
      else
        false
      end
    end

    # ---- the common surface ------------------------------------------------

    def publish(topic : String, payload = "", retain : Bool = false,
                qos : QoS = QoS::FireAndForget, timeout : Time::Span? = @timeout)
      case client = negotiated
      in V5::Client then client.publish(topic, payload, retain, qos, timeout: timeout)
      in V3::Client then client.publish(topic, payload, retain, qos, timeout)
      in MQTT::ClientBase
        # the union is exhaustive, the abstract base only satisfies the compiler
        raise "unreachable"
      end
      self
    end

    def subscribe(*topics, qos : QoS = QoS::FireAndForget, timeout : Time::Span? = @timeout,
                  &callback : String, Bytes, Bool -> Nil)
      filters = topics.to_a.flatten.map(&.to_s).uniq!

      case client = negotiated
      in V5::Client
        client.subscribe(filters, callback.as(V5::Client::Callback), qos: qos, timeout: timeout)
      in V3::Client
        mapped = {} of String => Tuple(QoS, V3::Client::Callback)
        filters.each { |filter| mapped[filter] = {qos, callback.as(V3::Client::Callback)} }
        client.subscribe(mapped, timeout)
      in MQTT::ClientBase
        # the union is exhaustive, the abstract base only satisfies the compiler
        raise "unreachable"
      end
      self
    end

    def unsubscribe(*topics, timeout : Time::Span? = @timeout)
      negotiated.unsubscribe(*topics, timeout: timeout)
      self
    end

    def ping(timeout : Time::Span? = @timeout) : Nil
      negotiated.ping(timeout)
    end

    def disconnect(send_msg = true) : Nil
      @client.try &.disconnect(send_msg)
    end

    def subscriptions : Hash(String, QoS)
      negotiated.subscriptions
    end

    def wait_close : Nil
      negotiated.wait_close
    end

    def closed? : Bool
      client = @client
      client.nil? || client.closed?
    end

    def terminated? : Bool
      client = @client
      client.nil? || client.terminated?
    end

    def last_ping_response : Time?
      @client.try &.last_ping_response
    end
  end
end
