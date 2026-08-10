require "../client_base"

# NOTE:: the packet classes this client is built from. It used to rely on the
# root aggregator's glob to pull them in, which meant requiring this file
# directly — the documented entry point — left them undefined
require "./*"

module MQTT
  module V3
    # https://test.mosquitto.org/
    #
    # The transport lifecycle, request pipeline, keep alive and reconnection all
    # live in `MQTT::ClientBase`. What follows is the 3.1.1 specific half:
    # building packets, parsing them, and what an acknowledgement means
    class Client < MQTT::ClientBase
      # Based on https://github.com/ralphtheninja/mqtt-match/blob/master/index.js
      def self.topic_matches(filter : String, topic : String)
        MQTT.topic_matches?(filter, topic)
      end

      # A subscription callback. The three argument form additionally receives
      # whether the broker flagged the message as retained, which is how you
      # tell stored state from a live update
      alias Callback = Proc(String, Bytes, Nil) | Proc(String, Bytes, Bool, Nil)

      # Everything we know about one topic filter. Kept together so a
      # reconnection can replay the subscription exactly as it was requested
      private class Subscription
        property requested : QoS
        property granted : QoS
        getter callbacks : Array(Callback)

        def initialize(@requested : QoS, granted : QoS? = nil)
          @granted = granted || @requested
          @callbacks = [] of Callback
        end
      end

      @subscriptions = {} of String => Subscription

      # Inbound QoS 2 messages held between PUBLISH and PUBREL
      @inbound_qos2 = {} of UInt16 => Publish

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

      # Drives the client over a single transport. The connection is not
      # retried if it drops, see the block form for that
      def initialize(
        transport : Transport,
        timeout : Time::Span? = DEFAULT_TIMEOUT,
        max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
      )
        super(transport, timeout, max_packet_size)
        start_transport
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
        timeout : Time::Span? = DEFAULT_TIMEOUT,
        max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
        reconnect : MQTT::Reconnect = MQTT::Reconnect.new,
        &factory : -> Transport
      )
        super(timeout, max_packet_size, factory, reconnect)
        start_transport
      end

      # The QoS the broker granted for each active subscription.
      # A broker is free to downgrade the level you asked for, so this is not
      # necessarily what was requested
      def subscriptions : Hash(String, QoS)
        @message_lock.synchronize { @subscriptions.transform_values(&.granted) }
      end

      # NOTE:: called with `@message_lock` held
      protected def reset_connection_state : Nil
        @inbound_qos2.clear
      end

      # ---- connecting --------------------------------------------------------

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

        pending = Pending(Header).new
        existing = @message_lock.synchronize do
          if current = @waiting_connect
            current
          else
            @waiting_connect = pending
            nil
          end
        end

        # a connection attempt is already in flight, wait on that one
        return existing.get(timeout, "connection acknowledgement").as(Connack) if existing

        connect = build_connect(
          username, password, keep_alive, client_id, clean_start,
          will_flag, will_qos, will_retain, will_topic, will_payload
        )

        Log.debug { "transport connection established, sending connect packet" }

        ack = begin
          transmit(connect, timeout)
          pending.get(timeout, "connection acknowledgement").as(Connack)
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
            @can_resume = true
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

      # Sends a PINGREQ and waits for the broker's PINGRESP
      def ping(timeout : Time::Span? = @timeout) : Nil
        ping = Pingreq.new
        ping.id = MQTT::RequestType::Pingreq
        ping.packet_length = ping.calculate_length

        pending = Pending(Header).new
        @message_lock.synchronize { @waiting_ping << pending }

        begin
          transmit(ping, timeout)
          pending.get(timeout, "ping response")
        ensure
          @message_lock.synchronize { @waiting_ping.delete(pending) }
        end
        nil
      end

      # ---- publishing --------------------------------------------------------

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

          pending = expect(RequestType::Pubcomp, message_id)
          begin
            transmit(release, timeout)
            pending.get(timeout, "PUBCOMP")
          ensure
            forget(RequestType::Pubcomp, message_id)
          end
        end

        self
      end

      # ---- subscribing -------------------------------------------------------

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
      def subscribe(topics : Hash(String, Tuple(QoS, Callback)), timeout : Time::Span? = @timeout)
        # MQTT-3.8.3-3, a SUBSCRIBE must carry at least one topic filter
        raise ArgumentError.new("at least one topic filter is required") if topics.empty?

        # Normalise once so the callback registry, the QoS registry and the
        # wire payload all agree on the key
        requested = {} of String => Tuple(QoS, Callback)
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

        message_id, pending = expect_next(RequestType::Suback)
        sub.message_id = message_id
        sub.packet_length = sub.calculate_length

        begin
          transmit(sub, timeout)
          ack = pending.get(timeout, "SUBACK").as(Suback)

          codes = ack.raw_return_codes
          if codes.size != filters.size
            raise SubscriptionError.new("broker returned #{codes.size} return codes for #{filters.size} topic filters")
          end
          ack
        ensure
          forget(RequestType::Suback, message_id)
        end
      end

      # Undoes the callback registration performed by `subscribe`
      private def remove_callbacks(
        requested : Hash(String, Tuple(QoS, Callback)),
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

      # NOTE:: the block may take two parameters (topic, payload) or three
      # (topic, payload, retained). Crystal lets a shorter block satisfy the
      # longer restriction, so existing two parameter blocks are unaffected
      def subscribe(*topics, qos : QoS = QoS::FireAndForget, timeout : Time::Span? = @timeout, &callback : String, Bytes, Bool -> Nil)
        mapped_topics = {} of String => Tuple(QoS, Callback)
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
      def unsubscribe(topic : String, callback : Callback, timeout : Time::Span? = @timeout)
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

      # ---- receiving ---------------------------------------------------------

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
          unless resolve(message_type, packet.message_id, packet)
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
          unless resolve(message_type, packet.message_id, packet)
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
          dispatch(pub.topic, pub.payload, pub.retain)
        in QoS::BrokerReceived
          acknowledge(RequestType::Puback, pub.message_id)
          dispatch(pub.topic, pub.payload, pub.retain)
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
          dispatch(held.topic, held.payload, held.retain)
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
      protected def dispatch(topic : String, payload : Bytes, retained : Bool = false) : Nil
        # snapshot under the lock, the callbacks themselves run outside of it
        matched = @message_lock.synchronize do
          @subscriptions.compact_map do |filter, subscription|
            {filter, subscription.callbacks.dup} if MQTT.topic_matches?(filter, topic)
          end
        end

        matched.each do |(filter, callbacks)|
          callbacks.each do |callback|
            case callback
            in Proc(String, Bytes, Nil)       then callback.call(topic, payload)
            in Proc(String, Bytes, Bool, Nil) then callback.call(topic, payload, retained)
            end
          rescue error
            Log.error(exception: error) { "callback failed #{filter} for #{topic}" }
          end
        end
      end
    end # Client
  end   # V3
end     # MQTT
