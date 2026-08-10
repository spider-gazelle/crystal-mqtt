require "../transport"
require "../pending"
require "../reconnect"
require "mutex"

module MQTT
  module V3
    # https://test.mosquitto.org/
    class Client
      # How long to wait for a broker response before giving up.
      # Previously every request waited forever
      DEFAULT_TIMEOUT = 30.seconds

      getter last_ping_response : Time? = nil

      # Applied to any request that isn't given an explicit timeout.
      # Set to `nil` to wait indefinitely
      property timeout : Time::Span?

      # Largest packet we're willing to buffer from the broker
      getter max_packet_size : UInt32

      @message_lock = Mutex.new
      @message_id = 0_u16

      # Based on https://github.com/ralphtheninja/mqtt-match/blob/master/index.js
      def self.topic_matches(filter : String, topic : String)
        filter_array = filter.split("/")
        # remove any MQTT shared subscription prefix
        # https://emqx.medium.com/introduction-to-mqtt-5-0-protocol-shared-subscription-4c23e7e0e3c1
        if filter_array.first? == "$share"
          filter_array = filter_array.size > 2 ? filter_array[2..] : [] of String
        end
        topic_array = topic.split("/")

        # Normalise the strings
        topic_array.shift if topic_array[0].empty?
        filter_array.shift if filter_array.first?.try(&.empty?)

        # MQTT-4.7.2-1: a wildcard at the first level must not match a topic
        # beginning with `$`, those are reserved for the broker
        if topic_array.first?.try(&.starts_with?('$'))
          leading = filter_array.first?
          return false if leading == "#" || leading == "+"
        end

        length = filter_array.size

        filter_array.each_with_index do |left, index|
          right = topic_array[index]?

          return (topic_array.size >= (length - 1)) if left == "#"
          return false if left != "+" && left != right
        end

        topic_array.size == length
      end

      # Packet identifiers must be non-zero and must not collide with a request
      # that is still in flight.
      # NOTE:: `@message_lock` must be held by the caller
      protected def next_message_id : UInt16
        UInt16::MAX.times do
          # Allow overflows, but skip 0 as MQTT-2.3.1-1 reserves it
          @message_id = @message_id &+ 1
          @message_id = 1_u16 if @message_id.zero?
          id = @message_id
          return id unless in_flight?(id)
        end

        raise Error.new("no packet identifiers available, too many requests in flight")
      end

      private def in_flight?(id : UInt16) : Bool
        return true if @waiting_suback.has_key?(id)
        @waiting_ack.each_key { |key| return true if key[1] == id }
        false
      end

      @waiting_ack = {} of Tuple(RequestType, UInt16) => Pending(Ack)
      @waiting_suback = {} of UInt16 => Pending(Suback)
      @waiting_ping = [] of Pending(EmptyPacket)
      @waiting_connect : Pending(Connack)? = nil

      # Inbound QoS 2 messages held between PUBLISH and PUBREL
      @inbound_qos2 = {} of UInt16 => Publish

      # Monotonic timestamp of the last packet sent or received, used to decide
      # whether a keep alive ping is required
      @last_activity : MQTT::Monotonic = MQTT.monotonic

      # Everything we know about one topic filter. Kept together so a
      # reconnection can replay the subscription exactly as it was requested
      private class Subscription
        property requested : QoS
        property granted : QoS
        getter callbacks : Array(Proc(String, Bytes, Nil))

        def initialize(@requested : QoS, granted : QoS? = nil)
          @granted = granted || @requested
          @callbacks = [] of Proc(String, Bytes, Nil)
        end
      end

      @subscriptions = {} of String => Subscription

      # The arguments of the last successful `connect`, replayed on reconnect
      @connect_options : NamedTuple(
        username: String?,
        password: String?,
        keep_alive: Int32,
        client_id: String,
        clean_start: Bool,
        will_flag: Bool,
        will_qos: QoS,
        will_retain: Bool,
        will_topic: String?,
        will_payload: (String | Bytes)?,
        keep_alive_active: Bool)? = nil

      # Builds a fresh transport when reconnecting. A socket cannot be reopened,
      # so reconnection needs a factory rather than the transport itself
      @transport_factory : Proc(Transport)?
      @reconnect : MQTT::Reconnect?

      # set once the client is finished for good, so a deliberate disconnect
      # isn't mistaken for a dropped connection
      @terminated : Bool = false
      @reconnecting : Bool = false

      # Drives the client over a single transport. The connection is not
      # retried if it drops, see the block form for that
      def initialize(
        transport : Transport,
        @timeout : Time::Span? = DEFAULT_TIMEOUT,
        @max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
      )
        @transport = transport
        @wait_close = Channel(Nil).new

        spawn(name: "mqtt-requests") { process_requests! }
        attach(transport)
      end

      # Drives the client over transports produced by the block, re-establishing
      # the connection (and its subscriptions) whenever it drops.
      #
      # ```
      # client = MQTT::V3::Client.new(reconnect: MQTT::Reconnect.new) do
      #   MQTT::Transport::TCP.new("test.mosquitto.org")
      # end
      # client.connect
      # ```
      def initialize(
        @timeout : Time::Span? = DEFAULT_TIMEOUT,
        @max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
        reconnect : MQTT::Reconnect = MQTT::Reconnect.new,
        &factory : -> Transport
      )
        @transport_factory = factory
        @reconnect = reconnect
        @transport = factory.call
        @wait_close = Channel(Nil).new

        spawn(name: "mqtt-requests") { process_requests! }
        attach(@transport)
      end

      @transport : Transport

      # Wires our callbacks up before the transport is allowed to produce data
      protected def attach(transport : Transport) : Nil
        transport.on_close do
          on_close(transport.error)
          nil
        end

        transport.on_tokenize { |buffer| tokenize(buffer) }

        transport.on_message do |data|
          parse_message(IO::Memory.new(data))
          nil
        end

        # Only safe to consume data once the callbacks above are configured
        begin
          transport.start
        rescue error : MQTT::Error
          raise error
        rescue error
          # keeps the promise that everything raised here is an MQTT::Error,
          # the underlying socket failure is preserved as the cause
          raise NotConnectedError.new("failed to establish the transport connection", error)
        end
      end

      protected def tokenize(buffer : IO::Memory) : Int32
        return -1 if buffer.size < 2

        header = begin
          buffer.read_bytes Header
        rescue
          # the variable length header isn't complete yet
          return -1
        end

        length = header.fixed_header_size.to_i64 + header.packet_length
        if length > @max_packet_size
          # without this a hostile broker can advertise a 256MB packet and make
          # us buffer all of it before a single message is dispatched
          Log.error { "packet of #{length} bytes exceeds the maximum packet size of #{@max_packet_size} bytes, closing connection" }
          @transport.close!
          return -1
        end

        length.to_i32
      end

      protected def on_close(error : ::Exception?)
        # Clean up the connection state here
        if error
          Log.error(exception: error) { "socket closed, error consuming IO" }
        else
          Log.debug { "socket closed, stopped processing incoming messages." }
        end

        # NOTE:: `cause` is set through the constructor. `Exception#cause=` only
        # existed here because the promise shard monkey patched it in
        failure = NotConnectedError.new("socket closed, stopped processing incoming messages.", error)

        # Collect under the lock, complete outside of it. A waiter woken by a
        # rejection may re-enter the client and `Mutex` is not re-entrant
        connecting = nil
        subacks = [] of Pending(Suback)
        acks = [] of Pending(Ack)
        pings = [] of Pending(EmptyPacket)

        @message_lock.synchronize do
          connecting = @waiting_connect
          @waiting_connect = nil
          subacks = @waiting_suback.values
          @waiting_suback.clear
          acks = @waiting_ack.values
          @waiting_ack.clear
          pings = @waiting_ping.dup
          @waiting_ping.clear
          @inbound_qos2.clear
        end

        connecting.try &.reject(failure)
        subacks.each &.reject(failure)
        acks.each &.reject(failure)
        pings.each &.reject(failure)

        if reconnect_wanted?
          spawn(name: "mqtt-reconnect") { reconnect! }
        else
          terminate!
        end
      end

      # Whether the connection dropped in a way we should recover from. A
      # deliberate `disconnect` is not one, and neither is a client that never
      # got a transport factory.
      # Claims the reconnect, so a transport closing mid-retry can't start a
      # second loop
      private def reconnect_wanted? : Bool
        return false if @terminated || @transport_factory.nil?

        @message_lock.synchronize do
          # nothing to re-establish until a connection has been made once
          next false if @reconnecting || @connect_options.nil?
          @reconnecting = true
        end
      end

      # Re-establishes the connection, and the session that was on it, using a
      # fresh transport from the factory
      protected def reconnect! : Nil
        factory = @transport_factory
        policy = @reconnect
        return terminate! unless factory && policy

        attempt = 0
        loop do
          attempt += 1
          if policy.give_up?(attempt)
            Log.error { "giving up reconnecting after #{attempt - 1} attempts" }
            break
          end

          delay = policy.delay_for(attempt)
          Log.info { "reconnecting in #{delay} (attempt #{attempt})" }
          sleep delay
          break if @terminated

          begin
            transport = factory.call
            @transport = transport
            attach(transport)
            resume_session
            Log.info { "reconnected after #{attempt} attempt(s)" }
            @message_lock.synchronize { @reconnecting = false }
            return
          rescue error
            Log.warn(exception: error) { "reconnect attempt #{attempt} failed" }
            # the transport's own close will not start a competing retry,
            # `@reconnecting` is still set
            @transport.close! rescue nil
          end
        end

        @message_lock.synchronize { @reconnecting = false }
        terminate!
      end

      # Replays the CONNECT that established the session, and the subscriptions
      # that were on it
      protected def resume_session : Nil
        options = @message_lock.synchronize { @connect_options }
        raise Error.new("no connection to resume") unless options

        ack = connect(**options)
        ack.success!

        # a broker that resumed our session already holds the subscriptions
        if ack.session_present
          Log.debug { "broker resumed the existing session" }
        else
          replay_subscriptions
        end
      end

      protected def replay_subscriptions : Nil
        filters = @message_lock.synchronize do
          @subscriptions.transform_values(&.requested)
        end
        return if filters.empty?

        Log.debug { "restoring #{filters.size} subscription(s)" }
        ack = send_subscribe(filters, @timeout)

        @message_lock.synchronize do
          filters.each_key.with_index do |filter, index|
            code = ack.raw_return_codes[index]
            next if ack.failure?(code)
            @subscriptions[filter]?.try &.granted = QoS.from_value(code)
          end
        end
      end

      # Stops the request processor and wakes anything in `wait_close`
      protected def terminate! : Nil
        @terminated = true
        @processor.close
        @wait_close.close
      end

      def terminated? : Bool
        @terminated
      end

      # Returns once the MQTT connection has terminated
      def wait_close : Nil
        @wait_close.receive?
      end

      # The QoS the broker granted for each active subscription.
      # A broker is free to downgrade the level you asked for, so this is not
      # necessarily what was requested
      def subscriptions : Hash(String, QoS)
        @message_lock.synchronize { @subscriptions.transform_values(&.granted) }
      end

      # Every packet we transmit is a `Header` subclass
      alias Request = Header

      # Carries the outcome of writing a packet back to the requesting fiber,
      # `nil` meaning the write succeeded
      alias SendResult = ::Channel(::Exception?)

      @processor = ::Channel(Tuple(Request, SendResult?)).new(8)

      protected def process_requests!
        Log.debug { "request processing has started..." }

        while received = @processor.receive?
          packet, result = received

          begin
            if @transport.closed?
              result.try &.send(NotConnectedError.new("socket closed"))
              next
            end

            Log.debug { "writing packet: #{packet.inspect}" }
            @transport.send(packet)
            @last_activity = MQTT.monotonic
            result.try &.send(nil)
          rescue e : IO::Error
            result.try &.send(Error.new("IO error", e))
          rescue e
            Log.error(exception: e) { "error processing request #{packet.id}" }
            result.try &.send(Error.new("unexpected error", e))
          end
        end
      ensure
        Log.debug { "request processing has stopped" }
      end

      # Queues a packet and blocks until it has been written to the transport
      protected def transmit(packet : Request, timeout : Time::Span?) : Nil
        result = SendResult.new(1)

        begin
          if timeout
            select
            when @processor.send({packet, result.as(SendResult?)})
              # queued
            when ::timeout(timeout)
              raise TimeoutError.new("timeout queueing #{packet.id} after #{timeout}")
            end
          else
            @processor.send({packet, result.as(SendResult?)})
          end
        rescue ::Channel::ClosedError
          raise NotConnectedError.new("client has disconnected")
        end

        error = if timeout
                  select
                  when value = result.receive
                    value
                  when ::timeout(timeout)
                    raise TimeoutError.new("timeout sending #{packet.id} after #{timeout}")
                  end
                else
                  result.receive
                end

        raise error if error
      end

      # Queues a packet without waiting for the write to complete. Used for
      # acknowledgements generated while handling an inbound message, so the
      # dispatch fiber is never blocked behind the socket
      protected def transmit_async(packet : Request) : Nil
        @processor.send({packet, nil.as(SendResult?)})
      rescue ::Channel::ClosedError
        # disconnected, nothing to acknowledge
      end

      def closed?
        @transport.closed?
      end

      # Negotiates the MQTT layer
      private def build_connect(
        username : String?,
        password : String?,
        keep_alive : Int32,
        client_id : String,
        clean_start : Bool,
        will_flag : Bool,
        will_qos : Int32 | QoS,
        will_retain : Bool,
        will_topic : String?,
        will_payload : (String | Bytes)?,
      ) : Connect
        connect = Connect.new
        connect.id = MQTT::RequestType::Connect
        connect.keep_alive_seconds = keep_alive.to_u16
        connect.client_id = client_id
        connect.clean_start = clean_start
        connect.username = username if username
        connect.password = password if password
        connect.will_flag = will_flag
        connect.will_qos = will_qos.is_a?(QoS) ? will_qos : QoS.from_value(will_qos)
        connect.will_retain = will_retain
        connect.will_topic = will_topic if will_topic
        connect.will_payload = will_payload if will_payload
        connect.packet_length = connect.calculate_length
        connect
      end

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
      )
        if will_flag && (will_topic.nil? || will_topic.empty?)
          # MQTT-3.1.3-10, a zero length will topic is a protocol violation
          raise ArgumentError.new("will_topic is required when will_flag is set")
        end

        pending = Pending(Connack).new
        existing = @message_lock.synchronize do
          if current = @waiting_connect
            current
          else
            @waiting_connect = pending
            nil
          end
        end

        # a connection attempt is already in flight, wait on that one
        return existing.get(timeout, "connection acknowledgement") if existing

        connect = build_connect(
          username, password, keep_alive, client_id, clean_start,
          will_flag, will_qos, will_retain, will_topic, will_payload
        )

        Log.debug { "transport connection established, sending connect packet" }

        ack = begin
          transmit(connect, timeout)
          pending.get(timeout, "connection acknowledgement")
        ensure
          # cleared either way so a failed connection can be retried
          @message_lock.synchronize { @waiting_connect = nil if @waiting_connect == pending }
        end

        if ack.success?
          # remembered so a dropped connection can be re-established exactly as
          # it was originally requested
          @message_lock.synchronize do
            @connect_options = {
              username:          username,
              password:          password,
              keep_alive:        keep_alive,
              client_id:         client_id,
              clean_start:       clean_start,
              will_flag:         will_flag,
              will_qos:          will_qos.is_a?(QoS) ? will_qos : QoS.from_value(will_qos),
              will_retain:       will_retain,
              will_topic:        will_topic,
              will_payload:      will_payload,
              keep_alive_active: keep_alive_active,
            }
          end
          start_keep_alive(keep_alive) if keep_alive_active
        else
          @transport.close!
        end
        ack
      end

      def disconnect(send_msg = true) : Nil
        # a deliberate disconnect must not trigger a reconnect
        @terminated = true
        return terminate! if closed?

        if send_msg
          disconnect = Disconnect.new
          disconnect.id = MQTT::RequestType::Disconnect
          disconnect.packet_length = disconnect.calculate_length

          begin
            transmit(disconnect, @timeout)
          rescue
            # ignore failures here, we'll just close the transport
          end
        end

        @transport.close!
      end

      # Sends a PINGREQ and waits for the broker's PINGRESP
      def ping(timeout : Time::Span? = @timeout) : Nil
        ping = Pingreq.new
        ping.id = MQTT::RequestType::Pingreq
        ping.packet_length = ping.calculate_length

        pending = Pending(EmptyPacket).new
        @message_lock.synchronize { @waiting_ping << pending }

        begin
          transmit(ping, timeout)
          pending.get(timeout, "ping response")
        ensure
          @message_lock.synchronize { @waiting_ping.delete(pending) }
        end
        nil
      end

      # Sends a PINGREQ whenever the link has been idle, so the broker doesn't
      # drop us for exceeding the keep alive interval we negotiated
      protected def start_keep_alive(seconds : Int32) : Nil
        return if seconds <= 0
        spawn(name: "mqtt-keepalive") { keep_alive!(seconds) }
      end

      protected def keep_alive!(seconds : Int32) : Nil
        # ping at 75% of the interval so a response has time to arrive
        interval = (seconds * 0.75).seconds

        loop do
          select
          when @wait_close.receive?
            break
          when ::timeout(interval)
            # time to check on the connection
          end
          break if closed?

          # no need to ping a link that has been busy
          next if (MQTT.monotonic - @last_activity) < interval

          begin
            ping(timeout: interval)
          rescue error
            Log.warn(exception: error) { "keep alive ping failed, closing connection" }
            @transport.close!
            break
          end
        end
      end

      def publish(
        topic : String,
        payload = "",
        retain : Bool = false,
        qos : QoS = QoS::FireAndForget,
        timeout : Time::Span? = @timeout,
      )
        raise ArgumentError.new("Topic name cannot be empty") if topic.empty?

        publish = Publish.new
        publish.id = MQTT::RequestType::Publish
        publish.qos = qos
        publish.topic = topic
        publish.retain = retain
        publish.payload = payload

        case qos
        in QoS::FireAndForget
          publish.packet_length = publish.calculate_length
          transmit(publish, timeout)
        in QoS::BrokerReceived
          # PUBLISH -> PUBACK
          reserve_ack(RequestType::Puback) do |id, pending|
            publish.message_id = id
            publish.packet_length = publish.calculate_length
            transmit(publish, timeout)
            pending.get(timeout, "PUBACK")
          end
        in QoS::SubscribersReceived
          # PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
          message_id = reserve_ack(RequestType::Pubrec) do |id, pending|
            publish.message_id = id
            publish.packet_length = publish.calculate_length
            transmit(publish, timeout)
            pending.get(timeout, "PUBREC")
            id
          end

          release = Pubrel.new
          release.id = MQTT::RequestType::Pubrel
          release.qos = required_qos(MQTT::RequestType::Pubrel)
          release.message_id = message_id
          release.packet_length = release.calculate_length

          pending = Pending(Ack).new
          @message_lock.synchronize { @waiting_ack[{RequestType::Pubcomp, message_id}] = pending }
          begin
            transmit(release, timeout)
            pending.get(timeout, "PUBCOMP")
          ensure
            @message_lock.synchronize { @waiting_ack.delete({RequestType::Pubcomp, message_id}) }
          end
        end

        self
      end

      # Allocates a message id, registers the expected response and guarantees
      # the registration is cleaned up however the block exits
      private def reserve_ack(expecting : RequestType, &)
        pending = Pending(Ack).new
        message_id = @message_lock.synchronize do
          id = next_message_id
          @waiting_ack[{expecting, id}] = pending
          id
        end

        begin
          yield message_id, pending
        ensure
          @message_lock.synchronize { @waiting_ack.delete({expecting, message_id}) }
        end

        message_id
      end

      # An empty filter is not a valid topic, treat it as the root topic
      private def normalise_filter(filter : String) : String
        filter.empty? ? "/" : filter
      end

      # SUBSCRIBE, UNSUBSCRIBE and PUBREL must be sent with QoS 1
      # (MQTT-3.8.1-1, MQTT-3.10.1-1 and MQTT-3.6.1-1)
      private def required_qos(type : RequestType) : QoS
        type.requires_qos? ? QoS::BrokerReceived : QoS::FireAndForget
      end

      # http://www.steves-internet-guide.com/understanding-mqtt-topics/
      def subscribe(topics : Hash(String, Tuple(QoS, Proc(String, Bytes, Nil))), timeout : Time::Span? = @timeout)
        # MQTT-3.8.3-3, a SUBSCRIBE must carry at least one topic filter
        raise ArgumentError.new("at least one topic filter is required") if topics.empty?

        # Normalise once so the callback registry, the QoS registry and the
        # wire payload all agree on the key
        requested = {} of String => Tuple(QoS, Proc(String, Bytes, Nil))
        topics.each { |filter, config| requested[normalise_filter(filter)] = config }
        filters = requested.keys

        # Register up front so a message arriving before we have processed the
        # SUBACK still reaches its callback
        @message_lock.synchronize do
          requested.each do |filter, (qos, proc)|
            subscription = (@subscriptions[filter] ||= Subscription.new(qos))
            subscription.requested = qos if qos > subscription.requested
            subscription.callbacks << proc
          end
        end

        begin
          # NOTE:: every requested filter is sent, even one we're already
          # subscribed to. Filtering the packet but not the response handling is
          # what used to misalign the return codes against the requested topics
          ack = send_subscribe(requested.transform_values { |(qos, _)| qos }, timeout)
          codes = ack.raw_return_codes

          # Record the granted QoS, tracking any filter the broker rejected
          rejected = [] of String
          @message_lock.synchronize do
            filters.each_with_index do |filter, index|
              code = codes[index]
              if ack.failure?(code)
                rejected << filter
              else
                @subscriptions[filter]?.try &.granted = QoS.from_value(code)
              end
            end
          end

          unless rejected.empty?
            remove_callbacks(requested, only: rejected)
            raise SubscriptionError.new("broker rejected subscription to #{rejected.join(", ")}")
          end
        rescue error
          Log.error(exception: error) { "error subscribing to topics" }

          # Remove callbacks that failed to configure
          remove_callbacks(requested)

          # NOTE:: this used to be swallowed, which reported success for a
          # subscription that was never established
          raise error
        end

        self
      end

      # Sends a SUBSCRIBE and returns the broker's SUBACK. Shared by `subscribe`
      # and the replay that follows a reconnect
      private def send_subscribe(filters : Hash(String, QoS), timeout : Time::Span?) : Suback
        sub = Subscribe.new
        sub.id = MQTT::RequestType::Subscribe
        sub.qos = required_qos(MQTT::RequestType::Subscribe)
        sub.topics = filters

        pending = Pending(Suback).new
        message_id = @message_lock.synchronize do
          id = next_message_id
          @waiting_suback[id] = pending
          id
        end

        sub.message_id = message_id
        sub.packet_length = sub.calculate_length

        begin
          transmit(sub, timeout)
          ack = pending.get(timeout, "SUBACK")

          codes = ack.raw_return_codes
          if codes.size != filters.size
            raise SubscriptionError.new("broker returned #{codes.size} return codes for #{filters.size} topic filters")
          end
          ack
        ensure
          @message_lock.synchronize { @waiting_suback.delete(message_id) }
        end
      end

      # Undoes the callback registration performed by `subscribe`
      private def remove_callbacks(
        requested : Hash(String, Tuple(QoS, Proc(String, Bytes, Nil))),
        only : Array(String)? = nil,
      ) : Nil
        @message_lock.synchronize do
          requested.each do |filter, (_, proc)|
            next if only && !only.includes?(filter)
            next unless subscription = @subscriptions[filter]?

            subscription.callbacks.delete(proc)
            @subscriptions.delete(filter) if subscription.callbacks.empty?
          end
        end
      end

      def subscribe(*topics, qos : QoS = QoS::FireAndForget, timeout : Time::Span? = @timeout, &callback : Proc(String, Bytes, Nil))
        mapped_topics = {} of String => Tuple(QoS, Proc(String, Bytes, Nil))
        topics.to_a.flatten.map(&.to_s).uniq!.each do |topic|
          mapped_topics[topic] = {qos, callback}
        end
        subscribe(mapped_topics, timeout)
        self
      end

      def unsubscribe(*topics, timeout : Time::Span? = @timeout)
        filters = topics.to_a.flatten.map { |topic| normalise_filter(topic.to_s) }.uniq!
        return self if filters.empty?

        @message_lock.synchronize { filters.each { |filter| @subscriptions.delete(filter) } }
        perform_unsubscribe(filters, timeout)
        self
      end

      # Removes a single callback, only unsubscribing once the last callback
      # for the filter has been removed
      def unsubscribe(topic : String, callback : Proc(String, Bytes, Nil), timeout : Time::Span? = @timeout)
        filter = normalise_filter(topic)
        removed = false

        @message_lock.synchronize do
          if subscription = @subscriptions[filter]?
            removed = !subscription.callbacks.delete(callback).nil?
            if subscription.callbacks.empty?
              @subscriptions.delete(filter)
            else
              # other callbacks still want this filter
              removed = false
            end
          end
        end

        perform_unsubscribe([filter], timeout) if removed
        self
      end

      protected def perform_unsubscribe(topics : Array(String), timeout : Time::Span? = @timeout) : Nil
        sub = Unsubscribe.new
        sub.id = MQTT::RequestType::Unsubscribe
        sub.qos = required_qos(MQTT::RequestType::Unsubscribe)
        sub.topics = topics

        reserve_ack(RequestType::Unsuback) do |id, pending|
          sub.message_id = id
          sub.packet_length = sub.calculate_length
          transmit(sub, timeout)
          pending.get(timeout, "UNSUBACK")
        end
      end

      def parse_message(io)
        @last_activity = MQTT.monotonic
        message_type = MQTT.peek_type(io)

        case message_type
        when RequestType::Connack
          packet = io.read_bytes Connack
          Log.debug { "received #{packet.inspect}" }
          if connect_waiting = @message_lock.synchronize { @waiting_connect }
            connect_waiting.resolve(packet)
          else
            Log.warn { "unexpected connection acknowledgement" }
          end
        when RequestType::Suback
          packet = io.read_bytes Suback
          Log.debug { "received #{packet.inspect}" }
          if pending = @message_lock.synchronize { @waiting_suback[packet.message_id]? }
            pending.resolve(packet)
          else
            Log.warn { "unexpected subscription acknowledgement, id #{packet.message_id}" }
          end
        when RequestType::Pubrel
          # completes an inbound QoS 2 delivery
          packet = io.read_bytes Ack
          Log.debug { "received #{packet.inspect}" }
          release_inbound(packet.message_id)
        when RequestType::Puback, RequestType::Unsuback, RequestType::Pubrec, RequestType::Pubcomp
          packet = io.read_bytes Ack
          Log.debug { "received #{packet.inspect}" }
          if pending = @message_lock.synchronize { @waiting_ack[{message_type, packet.message_id}]? }
            pending.resolve(packet)
          else
            Log.warn { "unexpected #{message_type}, id #{packet.message_id}" }
          end
        when RequestType::Pingresp
          Log.debug { "received ping response" }
          @last_ping_response = Time.utc
          packet = io.read_bytes EmptyPacket
          if pending = @message_lock.synchronize { @waiting_ping.shift? }
            pending.resolve(packet)
          end
        when RequestType::Publish
          packet = io.read_bytes Publish
          Log.debug { "received publish request #{packet.inspect}" }
          publish_received(packet)
        else
          raise ProtocolError.new("unexpected message type received #{message_type}")
        end
      rescue e
        Log.error(exception: e) { "failed to parse message" }
        @transport.close!
      end

      def publish_received(pub)
        case pub.qos
        in QoS::FireAndForget
          dispatch(pub.topic, pub.payload)
        in QoS::BrokerReceived
          acknowledge(RequestType::Puback, pub.message_id)
          dispatch(pub.topic, pub.payload)
        in QoS::SubscribersReceived
          # MQTT-4.3.3, hold the message until the broker releases it with
          # PUBREL so that a redelivery can't be dispatched twice
          duplicate = @message_lock.synchronize do
            already_held = @inbound_qos2.has_key?(pub.message_id)
            @inbound_qos2[pub.message_id] = pub
            already_held
          end
          Log.debug { "duplicate QoS 2 publish #{pub.message_id}, awaiting release" } if duplicate
          acknowledge(RequestType::Pubrec, pub.message_id)
        end
      end

      # Delivers a held QoS 2 message and completes the handshake
      protected def release_inbound(message_id : UInt16) : Nil
        held = @message_lock.synchronize { @inbound_qos2.delete(message_id) }
        acknowledge(RequestType::Pubcomp, message_id)

        if held
          dispatch(held.topic, held.payload)
        else
          Log.warn { "unexpected publish release, id #{message_id}" }
        end
      end

      protected def acknowledge(type : RequestType, message_id : UInt16) : Nil
        ack = Ack.new
        ack.id = type
        ack.qos = required_qos(type)
        ack.message_id = message_id
        ack.packet_length = ack.calculate_length
        transmit_async(ack)
      end

      # Invokes any callback whose filter matches the topic
      protected def dispatch(topic : String, payload : Bytes) : Nil
        # snapshot under the lock, the callbacks themselves run outside of it
        matched = @message_lock.synchronize do
          @subscriptions.compact_map do |filter, subscription|
            {filter, subscription.callbacks.dup} if Client.topic_matches(filter, topic)
          end
        end

        matched.each do |(filter, callbacks)|
          callbacks.each do |callback|
            callback.call(topic, payload)
          rescue error
            Log.error(exception: error) { "callback failed #{filter} for #{topic}" }
          end
        end
      end
    end # Client
  end   # V3
end     # MQTT
