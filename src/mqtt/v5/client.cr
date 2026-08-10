# NOTE:: the packet requires come first. They pull the root module in, which
# loads `ClientBase` cleanly. Requiring `../client_base` first re-enters
# `mqtt.cr` while it is still loading and `V3::Client`'s superclass resolves to
# nothing
require "./connect"
require "./publish"
require "./subscribe"
require "../client_base"

module MQTT
  module V5
    # An MQTT 5.0 client.
    #
    # The transport lifecycle, request pipeline, keep alive and reconnection all
    # live in `MQTT::ClientBase`. What follows is the 5.0 specific half.
    #
    # The substantive difference from 3.1.1 is that acknowledgements now carry a
    # reason: a PUBACK can say the publish was rejected, and a SUBACK can reject
    # individual filters. Those are raised rather than resolved
    class Client < MQTT::ClientBase
      def self.topic_matches(filter : String, topic : String)
        MQTT.topic_matches?(filter, topic)
      end

      # A subscription callback. Beyond the 3.1.1 forms, 5.0 messages carry
      # properties worth reaching, so a callback may take the whole packet
      alias Callback = Proc(String, Bytes, Nil) |
                       Proc(String, Bytes, Bool, Nil) |
                       Proc(Publish, Nil)

      # Everything we know about one topic filter, so a reconnect can replay it
      # exactly as requested
      private class Subscription
        property requested : QoS
        property granted : QoS
        property? no_local : Bool
        property? retain_as_published : Bool
        property retain_handling : RetainHandling
        property identifier : UInt32?
        getter callbacks : Array(Callback)

        def initialize(
          @requested : QoS,
          granted : QoS? = nil,
          @no_local : Bool = false,
          @retain_as_published : Bool = false,
          @retain_handling : RetainHandling = RetainHandling::SendAlways,
          @identifier : UInt32? = nil,
        )
          @granted = granted || @requested
          @callbacks = [] of Callback
        end

        def to_topic(filter : String) : SubTopic
          topic = SubTopic.new
          topic.filter = filter
          topic.qos = requested
          topic.no_local = no_local?
          topic.retain_as_published = retain_as_published?
          topic.retain_handling = retain_handling
          topic
        end
      end

      @subscriptions = {} of String => Subscription
      @inbound_qos2 = {} of UInt16 => Publish

      # Topic aliases are scoped to a connection and reset on every new one
      @inbound_aliases = {} of UInt16 => String

      # What the broker told us it will accept, from the CONNACK
      getter server_receive_maximum : UInt16 = 65_535_u16
      getter server_maximum_qos : QoS = QoS::SubscribersReceived
      getter server_maximum_packet_size : UInt32? = nil
      getter server_topic_alias_maximum : UInt16 = 0_u16
      getter? server_retain_available : Bool = true
      getter? server_wildcard_available : Bool = true
      getter? server_shared_subscriptions_available : Bool = true
      getter? server_subscription_identifiers_available : Bool = true

      # The identifier the broker assigned when we sent an empty one
      getter assigned_client_id : String? = nil

      # Why the broker closed the connection, when it said
      getter disconnect_reason : ReasonCode? = nil

      # Supplies the next authentication step for `authentication_method`.
      # Receives the broker's data and returns ours, or nil to stop
      property authenticator : Proc(Bytes?, Bytes?)? = nil

      @connect_options : NamedTuple(
        username: String?,
        password: String?,
        keep_alive: Int32,
        client_id: String,
        clean_start: Bool,
        session_expiry_interval: UInt32?,
        receive_maximum: UInt16?,
        will_flag: Bool,
        will_qos: QoS,
        will_retain: Bool,
        will_topic: String?,
        will_payload: (String | Bytes)?,
        will_delay_interval: UInt32?,
        keep_alive_active: Bool)? = nil

      def initialize(
        transport : Transport,
        timeout : Time::Span? = DEFAULT_TIMEOUT,
        max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
      )
        super(transport, timeout, max_packet_size)
        start_transport
      end

      def initialize(
        timeout : Time::Span? = DEFAULT_TIMEOUT,
        max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
        reconnect : MQTT::Reconnect = MQTT::Reconnect.new,
        &factory : -> Transport
      )
        super(timeout, max_packet_size, factory, reconnect)
        start_transport
      end

      def subscriptions : Hash(String, QoS)
        @message_lock.synchronize { @subscriptions.transform_values(&.granted) }
      end

      # NOTE:: called with `@message_lock` held. Topic aliases are scoped to a
      # connection, so carrying them across one would publish to whatever the
      # alias used to mean
      protected def reset_connection_state : Nil
        @inbound_qos2.clear
        @inbound_aliases.clear
      end

      # A broker that told us not to come back is not worth retrying
      protected def reconnect_permitted? : Bool
        reason = @disconnect_reason
        reason.nil? || !reason.fatal?
      end

      # ---- connecting --------------------------------------------------------

      def connect(
        username : String? = nil,
        password : String? = nil,
        keep_alive : Int32 = 60,
        client_id : String = MQTT.generate_client_id,
        clean_start : Bool = true,
        session_expiry_interval : UInt32? = nil,
        receive_maximum : UInt16? = nil,
        will_flag : Bool = false,
        will_qos : Int32 | QoS = 0,
        will_retain : Bool = false,
        will_topic : String? = nil,
        will_payload : (String | Bytes)? = nil,
        will_delay_interval : UInt32? = nil,
        timeout : Time::Span? = @timeout,
        keep_alive_active : Bool = true,
      ) : Connack
        if will_flag && (will_topic.nil? || will_topic.empty?)
          raise ArgumentError.new("will_topic is required when will_flag is set")
        end
        # narrowed by the guard above, but the compiler cannot see that inside
        # the conditional further down
        will_topic_value = will_topic

        pending = Pending(Header).new
        existing = @message_lock.synchronize do
          if current = @waiting_connect
            current
          else
            @waiting_connect = pending
            nil
          end
        end
        return existing.get(timeout, "connection acknowledgement").as(Connack) if existing

        packet = build_connect(
          username, password, keep_alive, client_id, clean_start,
          session_expiry_interval, receive_maximum, will_flag, will_qos,
          will_retain, will_topic_value, will_payload, will_delay_interval
        )

        ack = begin
          transmit(packet, timeout)
          pending.get(timeout, "connection acknowledgement").as(Connack)
        ensure
          @message_lock.synchronize { @waiting_connect = nil if @waiting_connect == pending }
        end

        if ack.success?
          adopt(ack)
          @message_lock.synchronize do
            @connect_options = {
              username:                username,
              password:                password,
              keep_alive:              keep_alive,
              client_id:               client_id,
              clean_start:             clean_start,
              session_expiry_interval: session_expiry_interval,
              receive_maximum:         receive_maximum,
              will_flag:               will_flag,
              will_qos:                will_qos.is_a?(QoS) ? will_qos : QoS.from_value(will_qos),
              will_retain:             will_retain,
              will_topic:              will_topic,
              will_payload:            will_payload,
              will_delay_interval:     will_delay_interval,
              keep_alive_active:       keep_alive_active,
            }
            @can_resume = true
            @disconnect_reason = nil
          end

          # MQTT-3.1.2.10, a broker may impose its own keep alive
          negotiated = ack.server_keep_alive.try(&.to_i) || keep_alive
          start_keep_alive(negotiated) if keep_alive_active
        else
          @transport.close!
        end
        ack
      end

      private def build_connect(
        username : String?, password : String?, keep_alive : Int32, client_id : String,
        clean_start : Bool, session_expiry_interval : UInt32?, receive_maximum : UInt16?,
        will_flag : Bool, will_qos : Int32 | QoS, will_retain : Bool, will_topic : String?,
        will_payload : (String | Bytes)?, will_delay_interval : UInt32?,
      ) : Connect
        packet = Connect.new
        packet.id = MQTT::RequestType::Connect
        packet.version = Version::V5
        packet.keep_alive_seconds = keep_alive.to_u16
        packet.client_id = client_id
        packet.clean_start = clean_start
        packet.username = username if username
        packet.password = password if password
        packet.session_expiry_interval = session_expiry_interval
        packet.receive_maximum = receive_maximum
        packet.maximum_packet_size = @max_packet_size

        if will_flag
          packet.will_flag = true
          packet.will_qos = will_qos.is_a?(QoS) ? will_qos : QoS.from_value(will_qos)
          packet.will_retain = will_retain
          packet.will_topic = will_topic if will_topic
          packet.will_payload = will_payload if will_payload
          packet.will_delay_interval = will_delay_interval
        end

        packet.packet_length = packet.calculate_length
        packet
      end

      # Records what the broker said it will and won't accept
      private def adopt(ack : Connack) : Nil
        @assigned_client_id = ack.assigned_client_identifier
        @server_receive_maximum = ack.receive_maximum
        @server_maximum_qos = ack.maximum_qos
        @server_maximum_packet_size = ack.maximum_packet_size
        @server_topic_alias_maximum = ack.topic_alias_maximum
        @server_retain_available = ack.retain_available?
        @server_wildcard_available = ack.wildcard_subscription_available?
        @server_shared_subscriptions_available = ack.shared_subscription_available?
        @server_subscription_identifiers_available = ack.subscription_identifier_available?
      end

      def disconnect(send_msg = true, reason : ReasonCode = ReasonCode::Success) : Nil
        @terminated = true
        return terminate! if closed?

        if send_msg
          packet = Disconnect.new
          packet.id = MQTT::RequestType::Disconnect
          packet.reason_code = reason
          packet.packet_length = packet.calculate_length

          begin
            transmit(packet, @timeout)
          rescue
            # ignore, we're closing the transport regardless
          end
        end

        @transport.close!
      end

      protected def resume_session : Nil
        options = @message_lock.synchronize { @connect_options }
        raise Error.new("no connection to resume") unless options

        ack = connect(**options)
        ack.success!

        if ack.session_present
          Log.debug { "broker resumed the existing session" }
        else
          replay_subscriptions
        end
      end

      protected def replay_subscriptions : Nil
        snapshot = @message_lock.synchronize { @subscriptions.dup }
        return if snapshot.empty?

        Log.debug { "restoring #{snapshot.size} subscription(s)" }
        ack = send_subscribe(snapshot.map { |filter, sub| sub.to_topic(filter) }, nil, @timeout)

        @message_lock.synchronize do
          snapshot.each_key.with_index do |filter, index|
            code = ack.raw_reason_codes[index]
            next if ack.failure?(code)
            granted = ReasonCode.from_value?(code).try(&.granted_qos)
            @subscriptions[filter]?.try &.granted = granted if granted
          end
        end
      end

      def ping(timeout : Time::Span? = @timeout) : Nil
        packet = Pingreq.new
        packet.id = MQTT::RequestType::Pingreq
        packet.packet_length = packet.calculate_length

        pending = Pending(Header).new
        @message_lock.synchronize { @waiting_ping << pending }

        begin
          transmit(packet, timeout)
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
        content_type : String? = nil,
        response_topic : String? = nil,
        correlation_data : Bytes? = nil,
        message_expiry_interval : UInt32? = nil,
        payload_format_indicator : UInt8? = nil,
        user_properties : Enumerable(Tuple(String, String))? = nil,
        timeout : Time::Span? = @timeout,
      )
        raise ArgumentError.new("Topic name cannot be empty") if topic.empty?

        # fail locally rather than being disconnected for it
        if retain && !server_retain_available?
          raise ProtocolError.new("the broker does not support retained messages")
        end
        if qos > @server_maximum_qos
          raise ProtocolError.new("the broker's maximum QoS is #{@server_maximum_qos}")
        end

        packet = Publish.new
        packet.id = MQTT::RequestType::Publish
        packet.qos = qos
        packet.topic = topic
        packet.retain = retain
        packet.payload = payload
        packet.content_type = content_type
        packet.response_topic = response_topic
        packet.correlation_data = correlation_data
        packet.message_expiry_interval = message_expiry_interval
        packet.payload_format_indicator = payload_format_indicator
        user_properties.try &.each { |(key, value)| packet.add_user_property(key, value) }

        case qos
        in QoS::FireAndForget
          packet.packet_length = packet.calculate_length
          transmit(packet, timeout)
        in QoS::BrokerReceived
          reserve_ack(RequestType::Puback) do |id, pending|
            packet.message_id = id
            packet.packet_length = packet.calculate_length
            transmit(packet, timeout)
            check!(pending.get(timeout, "PUBACK").as(Ack), "publish")
          end
        in QoS::SubscribersReceived
          # PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
          received = nil
          message_id = reserve_ack(RequestType::Pubrec) do |id, pending|
            packet.message_id = id
            packet.packet_length = packet.calculate_length
            transmit(packet, timeout)
            received = pending.get(timeout, "PUBREC").as(Ack)
          end

          # MQTT-4.3.3, a PUBREC that reports an error ends the exchange. There
          # is nothing to release, so no PUBREL is sent
          check!(received.as(Ack), "publish")

          release = Pubrel.new
          release.id = MQTT::RequestType::Pubrel
          release.qos = QoS::BrokerReceived
          release.message_id = message_id
          release.packet_length = release.calculate_length

          pending = expect(RequestType::Pubcomp, message_id)
          begin
            transmit(release, timeout)
            check!(pending.get(timeout, "PUBCOMP").as(Ack), "publish release")
          ensure
            forget(RequestType::Pubcomp, message_id)
          end
        end

        self
      end

      # An acknowledgement can now report failure, which 3.1.1 had no way to say
      private def check!(ack : Ack, action : String) : Ack
        return ack if ack.success?

        detail = ack.reason_string
        message = "#{action} rejected: #{ack.reason_description}"
        message += " (#{detail})" if detail
        raise ProtocolError.new(message)
      end

      # ---- subscribing -------------------------------------------------------

      private def normalise_filter(filter : String) : String
        filter.empty? ? "/" : filter
      end

      def subscribe(
        *topics,
        qos : QoS = QoS::FireAndForget,
        no_local : Bool = false,
        retain_as_published : Bool = false,
        retain_handling : RetainHandling = RetainHandling::SendAlways,
        identifier : UInt32? = nil,
        timeout : Time::Span? = @timeout,
        &callback : String, Bytes, Bool -> Nil
      )
        filters = topics.to_a.flatten.map(&.to_s).uniq!
        subscribe(filters, callback, qos: qos, no_local: no_local,
          retain_as_published: retain_as_published, retain_handling: retain_handling,
          identifier: identifier, timeout: timeout)
      end

      # Full form, also used by the block version above
      def subscribe(
        filters : Enumerable(String),
        callback : Callback,
        qos : QoS = QoS::FireAndForget,
        no_local : Bool = false,
        retain_as_published : Bool = false,
        retain_handling : RetainHandling = RetainHandling::SendAlways,
        identifier : UInt32? = nil,
        timeout : Time::Span? = @timeout,
      )
        wanted = filters.map { |filter| normalise_filter(filter) }.to_a
        raise ArgumentError.new("at least one topic filter is required") if wanted.empty?

        if identifier && !server_subscription_identifiers_available?
          raise ProtocolError.new("the broker does not support subscription identifiers")
        end

        # Register before sending, so a message arriving with the SUBACK still
        # reaches its callback
        @message_lock.synchronize do
          wanted.each do |filter|
            subscription = (@subscriptions[filter] ||= Subscription.new(
              qos, no_local: no_local, retain_as_published: retain_as_published,
              retain_handling: retain_handling, identifier: identifier))
            subscription.requested = qos if qos > subscription.requested
            subscription.callbacks << callback
          end
        end

        begin
          requested = @message_lock.synchronize do
            wanted.map { |filter| @subscriptions[filter].to_topic(filter) }
          end
          ack = send_subscribe(requested, identifier, timeout)

          rejected = [] of String
          @message_lock.synchronize do
            wanted.each_with_index do |filter, index|
              code = ack.raw_reason_codes[index]
              if ack.failure?(code)
                rejected << filter
              elsif granted = ReasonCode.from_value?(code).try(&.granted_qos)
                @subscriptions[filter]?.try &.granted = granted
              end
            end
          end

          unless rejected.empty?
            remove_callbacks(wanted, callback, only: rejected)
            reasons = rejected.map_with_index { |filter, index| "#{filter}: #{ack.reason_codes[index]}" }
            raise SubscriptionError.new("broker rejected subscription to #{reasons.join(", ")}")
          end
        rescue error
          Log.error(exception: error) { "error subscribing to topics" }
          remove_callbacks(wanted, callback)
          raise error
        end

        self
      end

      private def send_subscribe(requested : Array(SubTopic), identifier : UInt32?, timeout : Time::Span?) : Suback
        sub = Subscribe.new
        sub.id = MQTT::RequestType::Subscribe
        sub.qos = QoS::BrokerReceived
        sub.subscription_identifier = identifier
        sub.topics = requested

        message_id, pending = expect_next(RequestType::Suback)
        sub.message_id = message_id
        sub.packet_length = sub.calculate_length

        begin
          transmit(sub, timeout)
          ack = pending.get(timeout, "SUBACK").as(Suback)

          if ack.raw_reason_codes.size != requested.size
            raise SubscriptionError.new("broker returned #{ack.raw_reason_codes.size} reason codes for #{requested.size} topic filters")
          end
          ack
        ensure
          forget(RequestType::Suback, message_id)
        end
      end

      private def remove_callbacks(filters : Array(String), callback : Callback, only : Array(String)? = nil) : Nil
        @message_lock.synchronize do
          filters.each do |filter|
            next if only && !only.includes?(filter)
            next unless subscription = @subscriptions[filter]?

            subscription.callbacks.delete(callback)
            @subscriptions.delete(filter) if subscription.callbacks.empty?
          end
        end
      end

      def unsubscribe(*topics, timeout : Time::Span? = @timeout)
        filters = topics.to_a.flatten.map { |topic| normalise_filter(topic.to_s) }.uniq!
        return self if filters.empty?

        @message_lock.synchronize { filters.each { |filter| @subscriptions.delete(filter) } }
        perform_unsubscribe(filters, timeout)
        self
      end

      protected def perform_unsubscribe(topics : Array(String), timeout : Time::Span? = @timeout) : Nil
        sub = Unsubscribe.new
        sub.id = MQTT::RequestType::Unsubscribe
        sub.qos = QoS::BrokerReceived
        sub.topics = topics

        reserve_ack(RequestType::Unsuback) do |id, pending|
          sub.message_id = id
          sub.packet_length = sub.calculate_length
          transmit(sub, timeout)
          ack = pending.get(timeout, "UNSUBACK").as(Unsuback)

          failed = topics.each_with_index.select do |_, index|
            code = ack.raw_reason_codes[index]?
            code ? ack.failure?(code) : false
          end.map(&.first).to_a

          unless failed.empty?
            raise SubscriptionError.new("broker rejected unsubscribe from #{failed.join(", ")}")
          end
        end
      end

      # ---- receiving ---------------------------------------------------------

      def parse_message(io)
        @last_activity = MQTT.monotonic
        message_type = MQTT.peek_type(io)

        case message_type
        when RequestType::Publish
          publish_received(io.read_bytes(Publish).validate!)
        when RequestType::Pubrel
          release_inbound(io.read_bytes(Ack).validate!.message_id)
        when RequestType::Disconnect
          # 5.0 lets the broker say why before it closes, which 3.1.1 could not
          server_disconnect(io.read_bytes(Disconnect).validate!)
        when RequestType::Auth
          authenticate(io.read_bytes(Auth).validate!)
        when RequestType::Pingresp
          @last_ping_response = Time.utc
          packet = io.read_bytes EmptyPacket
          if pending = @message_lock.synchronize { @waiting_ping.shift? }
            pending.resolve(packet)
          end
        else
          acknowledgement_received(message_type, io)
        end
      rescue e
        Log.error(exception: e) { "failed to parse message" }
        @transport.close!
      end

      # Everything that completes a request we are waiting on
      private def acknowledgement_received(message_type : RequestType, io) : Nil
        case message_type
        when RequestType::Connack
          packet = io.read_bytes(Connack).validate!
          Log.debug { "received #{packet.inspect}" }
          if waiting = @message_lock.synchronize { @waiting_connect }
            waiting.resolve(packet)
          else
            Log.warn { "unexpected connection acknowledgement" }
          end
        when RequestType::Suback
          resolve_or_warn(message_type, io.read_bytes(Suback).validate!)
        when RequestType::Unsuback
          resolve_or_warn(message_type, io.read_bytes(Unsuback).validate!)
        when RequestType::Puback, RequestType::Pubrec, RequestType::Pubcomp
          resolve_or_warn(message_type, io.read_bytes(Ack).validate!)
        else
          raise ProtocolError.new("unexpected message type received #{message_type}")
        end
      end

      private def resolve_or_warn(message_type : RequestType, packet) : Nil
        Log.debug { "received #{packet.inspect}" }
        return if resolve(message_type, packet.message_id, packet)
        Log.warn { "unexpected #{message_type}, id #{packet.message_id}" }
      end

      protected def server_disconnect(packet : Disconnect) : Nil
        @disconnect_reason = packet.reason_code
        detail = packet.reason_string
        Log.warn { "broker closed the connection: #{packet.reason_description}#{detail ? " (#{detail})" : ""}" }

        if reference = packet.server_reference
          Log.warn { "broker referred us to #{reference}" }
        end

        @transport.close!
      end

      # Enhanced authentication, MQTT-4.12. The broker challenges, the
      # authenticator answers, until it is satisfied or gives up
      protected def authenticate(packet : Auth) : Nil
        handler = @authenticator
        unless handler
          Log.error { "broker requested authentication but no authenticator is configured" }
          @transport.close!
          return
        end

        response = handler.call(packet.authentication_data)
        unless response
          Log.debug { "authenticator produced no further data" }
          return
        end

        reply = Auth.new
        reply.id = MQTT::RequestType::Auth
        reply.reason_code = ReasonCode::ContinueAuthentication
        reply.authentication_method = packet.authentication_method
        reply.authentication_data = response
        reply.packet_length = reply.calculate_length
        transmit_async(reply)
      end

      def publish_received(pub : Publish)
        resolve_alias(pub)

        case pub.qos
        in QoS::FireAndForget
          dispatch(pub)
        in QoS::BrokerReceived
          acknowledge(RequestType::Puback, pub.message_id)
          dispatch(pub)
        in QoS::SubscribersReceived
          duplicate = @message_lock.synchronize do
            already = @inbound_qos2.has_key?(pub.message_id)
            @inbound_qos2[pub.message_id] = pub
            already
          end
          Log.debug { "duplicate QoS 2 publish #{pub.message_id}, awaiting release" } if duplicate
          acknowledge(RequestType::Pubrec, pub.message_id)
        end
      end

      # A broker may replace the topic with an alias it established earlier
      private def resolve_alias(pub : Publish) : Nil
        return unless topic_alias = pub.topic_alias

        @message_lock.synchronize do
          if pub.topic.empty?
            known = @inbound_aliases[topic_alias]?
            raise ProtocolError.new("broker used unknown topic alias #{topic_alias}") unless known
            pub.topic = known
          else
            @inbound_aliases[topic_alias] = pub.topic
          end
        end
      end

      protected def release_inbound(message_id : UInt16) : Nil
        held = @message_lock.synchronize { @inbound_qos2.delete(message_id) }
        acknowledge(RequestType::Pubcomp, message_id)

        if held
          dispatch(held)
        else
          Log.warn { "unexpected publish release, id #{message_id}" }
        end
      end

      protected def acknowledge(type : RequestType, message_id : UInt16, reason : ReasonCode = ReasonCode::Success) : Nil
        ack = Ack.new
        ack.id = type
        ack.qos = QoS::BrokerReceived if type.pubrel?
        ack.message_id = message_id
        ack.reason_code = reason
        ack.packet_length = ack.calculate_length
        transmit_async(ack)
      end

      protected def dispatch(pub : Publish) : Nil
        topic = pub.topic
        identifiers = pub.subscription_identifiers

        matched = @message_lock.synchronize do
          @subscriptions.compact_map do |filter, subscription|
            # MQTT-3.3.4, when the broker tags a message with the subscription
            # that caused it we can route on that instead of re-matching
            wanted = if !identifiers.empty? && subscription.identifier
                       identifiers.includes?(subscription.identifier)
                     else
                       MQTT.topic_matches?(filter, topic)
                     end
            {filter, subscription.callbacks.dup} if wanted
          end
        end

        matched.each do |(filter, callbacks)|
          callbacks.each do |callback|
            case callback
            in Proc(String, Bytes, Nil)       then callback.call(topic, pub.payload)
            in Proc(String, Bytes, Bool, Nil) then callback.call(topic, pub.payload, pub.retain)
            in Proc(Publish, Nil)             then callback.call(pub)
            end
          rescue error
            Log.error(exception: error) { "callback failed #{filter} for #{topic}" }
          end
        end
      end
    end # Client
  end   # V5
end     # MQTT
