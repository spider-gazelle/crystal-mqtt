require "../transport"
require "promise"
require "mutex"

module MQTT
  module V3
    # https://test.mosquitto.org/
    class Client
      getter last_ping_response : Time? = nil

      @message_lock = Mutex.new
      @message_id = 0_u16

      protected def next_message_id
        # Allow overflows
        @message_id = @message_id &+ 1
      end

      @waiting_ack = {} of UInt16 => Promise::DeferredPromise(Ack)
      @waiting_suback = {} of UInt16 => Promise::DeferredPromise(Suback)
      @waiting_connect : Promise::DeferredPromise(Connack)? = nil

      def initialize(@transport : Transport)
        # Configure the subscription callback config
        @subscription_qos = {} of String => QoS
        @subscription_cbs = Hash(String, Array(Proc(String, Bytes, Nil))).new do |h, k|
          h[k] = [] of Proc(String, Bytes, Nil)
        end

        # Closes when the socket disconnects
        @wait_close = Channel(Nil).new

        # Hook up transport
        @transport.on_close do
          on_close(@transport.error)
          nil
        end

        @transport.on_tokenize { |buffer| tokenize(buffer) }

        @transport.on_message do |data|
          parse_message(IO::Memory.new(data))
          nil
        end

        spawn { self.process_requests! }
      end

      protected def tokenize(buffer : IO::Memory) : Int32
        return -1 if buffer.size < 2
        begin
          header = buffer.read_bytes Header
          header.to_slice.size + header.packet_length
        rescue
          -1
        end
      end

      protected def on_close(error : ::Exception?)
        # Clean up the connection state here
        if error
          Log.error(exception: error) { "Socket closed, error consuming IO\n#{error.inspect_with_backtrace}" }
        else
          Log.debug { "Socket closed, stopped processing incoming messages." }
        end

        error = IO::Error.new("Socket closed, stopped processing incoming messages.")
        @message_lock.synchronize do
          @waiting_connect.try &.reject(error)
          @waiting_connect = nil
          @waiting_suback.each_value { |value| value.reject(error) }
          @waiting_suback.clear
          @waiting_ack.each_value { |value| value.reject(error) }
          @waiting_ack.clear
          @wait_close.close
        end
      end

      # Returns once the MQTT connection has terminated
      def wait_close : Nil
        channel = @message_lock.synchronize { @wait_close }
        channel.receive? unless channel.closed?
      end

      # NOTE:: Unsubscribe is the same class as Subscribe
      alias Request = Connect | Publish | Subscribe | EmptyPacket
      @processor = ::Channel(Tuple(Request, Promise::DeferredPromise(Connack) | Promise::DeferredPromise(Suback) | Promise::DeferredPromise(Ack) | Promise::DeferredPromise(Bool))).new(8)

      protected def process_requests!
        Log.debug { "request processing has started..." }

        loop do
          received = @processor.receive?

          # nil if the channel was closed
          break unless received
          packet, promise = received

          begin
            if !@transport.closed?
              Log.debug { "writing packet: #{packet.inspect}" }
              @transport.send(packet)

              # If not expecting a response we'll resolve it after sending
              if promise.is_a?(Promise::DeferredPromise(Bool))
                promise.resolve(true)
              else
                # TODO:: implement timeouts
              end
            else
              promise.reject(NotConnectedError.new("socket closed"))
            end
          rescue e : IO::Error
            promise.reject(Error.new("IO error", e)) if promise
          rescue e
            Log.error(exception: e) { "error processing request #{packet.id}" }
            promise.reject(Error.new("unexpected error", e)) if promise
          end
        end
      ensure
        Log.debug { "request processing has stopped" }
      end

      def closed?
        @transport.closed?
      end

      def connect(
        username : String? = nil,
        password : String? = nil,
        keep_alive : Int32 = 60,
        client_id : String = MQTT.generate_client_id,
        clean_start : Bool = true
      )
        if connecting = @waiting_connect
          return connecting.get
        end

        # Negotiate the MQTT layer
        connect = Connect.new
        connect.id = MQTT::RequestType::Connect
        connect.keep_alive_seconds = keep_alive.to_u16
        connect.client_id = client_id
        connect.clean_start = true
        connect.username = username if username
        connect.password = password if password
        connect.packet_length = connect.calculate_length

        Log.debug { "TCP connection established, sending connect packet" }

        promise = Promise::DeferredPromise(Connack).new
        @message_lock.synchronize { @waiting_connect = promise }
        @processor.send({connect, promise})

        ack = promise.get
        @transport.close! unless ack.success?
        ack
      end

      def disconnect(send_msg = true) : Nil
        return if closed?

        if send_msg
          disconnect = Disconnect.new
          disconnect.id = MQTT::RequestType::Disconnect
          disconnect.packet_length = disconnect.calculate_length

          promise = Promise::DeferredPromise(Bool).new
          @processor.send({disconnect, promise})
          begin
            promise.get
          rescue
            # ignore failures here, we'll just close the transport
          end
        end

        @transport.close!
      end

      def publish(topic : String, payload = "", retain : Bool = false, qos : QoS = QoS::FireAndForget)
        raise ArgumentError.new("Topic name cannot be empty") if topic.empty?

        publish = Publish.new
        publish.id = MQTT::RequestType::Publish
        publish.qos = qos
        publish.topic = topic
        publish.retain = retain
        publish.payload = payload
        publish.packet_length = publish.calculate_length

        if publish.qos?
          promise = Promise::DeferredPromise(Puback).new
          publish.message_id = @message_lock.synchronize do
            next_id = next_message_id
            @waiting_ack[next_id] = promise
            next_id
          end
        else
          promise = Promise::DeferredPromise(Bool).new
        end

        @processor.send({publish, promise})
        promise.get
        self
      end

      # http://www.steves-internet-guide.com/understanding-mqtt-topics/
      def subscribe(topics : Hash(String, Tuple(QoS, Proc(String, Bytes, Nil))))
        # Build the message
        sub = Subscribe.new
        sub.id = MQTT::RequestType::Subscribe
        sub.qos = QoS::BrokerReceived
        sub.message_id = @message_lock.synchronize { next_message_id }

        # Don't re-subscribe to a topic unless it has a greater QoS
        to_configure = {} of String => QoS
        @message_lock.synchronize do
          topics.each do |key, value|
            qos = value[0]
            key = "/" if key.empty?

            # This check requires the lock
            if existing_qos = @subscription_qos[key]?
              to_configure[key] = qos if existing_qos < qos
            else
              to_configure[key] = qos
            end
          end
        end
        sub.topics = to_configure
        sub.packet_length = sub.calculate_length

        begin
          # Configure the request
          promise = Promise::DeferredPromise(Suback).new
          sub.message_id = @message_lock.synchronize do
            next_id = next_message_id
            @waiting_suback[next_id] = promise

            # Update the callbacks
            topics.each do |topic, (_, proc)|
              @subscription_cbs[topic] << proc
            end

            # Return the message id we were after
            next_id
          end

          # Make the request and wait for the response
          @processor.send({sub, promise})
          ack = promise.get
          return_codes = ack.return_codes

          # Configure the callback QoS
          index = 0
          @message_lock.synchronize do
            topics.each_key do |topic|
              ack_qos = return_codes[index]
              @subscription_qos[topic] = ack_qos
              index += 1
            end
          end
        rescue error
          Log.error(exception: error) { "error subscribing to topics\n#{error.inspect_with_backtrace}" }

          # Remove callbacks that failed to configure
          @message_lock.synchronize do
            topics.each do |topic, (_, proc)|
              array = @subscription_cbs[topic]
              array.delete(proc)
              if array.empty?
                @subscription_cbs.delete(topic)
                @subscription_qos.delete(topic)
              end
            end
          end
        end

        self
      end

      def subscribe(*topics, qos : QoS = QoS::FireAndForget, &callback : Proc(String, Bytes, Nil))
        mapped_topics = {} of String => Tuple(QoS, Proc(String, Bytes, Nil))
        topics.to_a.flatten.map(&.to_s).uniq.each do |topic|
          mapped_topics[topic] = {qos, callback}
        end
        subscribe(mapped_topics)
        self
      end

      def unsubscribe(*topics)
        topics = topics.to_a.flatten.map(&.to_s).uniq
        @message_lock.synchronize do
          topics.each do |topic|
            @subscription_cbs.delete(topic)
            @subscription_qos.delete(topic)
          end
        end
        perform_unsubscribe(topics)
        self
      end

      def unsubscribe(topic : String, callback : Proc(String, Bytes, Nil))
        found = false

        @message_lock.synchronize do
          array = @subscription_cbs[topic]
          cb = array.delete(proc)
          if array.empty?
            found = !!cb
            @subscription_cbs.delete(topic)
            @subscription_qos.delete(topic)
          end
        end
        perform_unsubscribe([topic]) if found
        self
      end

      protected def perform_unsubscribe(topics : Array(String)) : Nil
        sub = Unsubscribe.new
        sub.id = MQTT::RequestType::Unsubscribe
        sub.qos = QoS::BrokerReceived
        sub.topics = topics
        sub.packet_length = sub.calculate_length

        promise = Promise::DeferredPromise(Unsuback).new
        sub.message_id = @message_lock.synchronize do
          next_id = next_message_id
          @waiting_ack[next_id] = promise
          next_id
        end
        @processor.send({sub, promise})
        promise.get
      end

      def ping : Nil
        ping = Pingreq.new
        ping.id = MQTT::RequestType::Pingreq
        ping.packet_length = ping.calculate_length

        promise = Promise::DeferredPromise(Bool).new
        @processor.send({ping, promise})
        promise.get
      end

      def parse_message(io)
        message_type = MQTT.peek_type(io)
        case message_type
        when RequestType::Connack
          packet = io.read_bytes Connack
          packet.packet_length
          Log.debug { "received #{packet.inspect}" }
          if connect_waiting = @message_lock.synchronize { @waiting_connect }
            connect_waiting.resolve(packet)
          else
            Log.warn { "unexpected connection acknowledgement" }
          end
        when RequestType::Suback
          packet = io.read_bytes Suback
          packet.packet_length
          Log.debug { "received #{packet.inspect}" }
          if promise = @message_lock.synchronize { @waiting_suback.delete(packet.message_id) }
            promise.resolve(packet)
          else
            Log.warn { "unexpected subscription acknowledgement, id #{packet.message_id}" }
          end
        when RequestType::Puback, RequestType::Unsuback, RequestType::Pubrec, RequestType::Pubrel, RequestType::Pubcomp
          packet = io.read_bytes Ack
          packet.packet_length
          Log.debug { "received #{packet.inspect}" }
          if promise = @message_lock.synchronize { @waiting_ack.delete(packet.message_id) }
            promise.resolve(packet)
          else
            Log.warn { "unexpected #{message_type}, id #{packet.message_id}" }
          end
        when RequestType::Pingresp
          # Do nothing
          Log.debug { "received ping response" }
          @last_ping_response = Time.utc
        when RequestType::Publish
          packet = io.read_bytes Publish
          packet.packet_length
          Log.debug { "received publish request #{packet.inspect}" }
          publish_received(packet)
        else
          Log.error { "invalid message type received #{message_type}" }
        end
      rescue e
        Log.error(exception: e) { "failed to parse message: #{io.to_slice}" }
        @transport.close!
      end

      def publish_received(pub)
        topic = pub.topic
        payload = pub.payload

        # Send the ack
        if pub.qos?
          begin
            ack = Puback.new
            ack.id = MQTT::RequestType::Puback
            ack.message_id = pub.message_id
            ack.packet_length = ack.calculate_length
            promise = Promise::DeferredPromise(Bool).new

            @processor.send({ack, promise})
            promise.get
          rescue error
            Log.error(exception: error) { "failed to send ack for #{pub.message_id}" }
          end
        end

        @subscription_cbs.each do |filter, callbacks|
          if topic_matches(filter, topic)
            callbacks.each do |callback|
              begin
                callback.call(topic, payload)
              rescue error
                Log.error(exception: error) { "callback failed #{filter} for #{topic}" }
              end
            end
          end
        end
      end

      # Based on https://github.com/ralphtheninja/mqtt-match/blob/master/index.js
      def topic_matches(filter : String, topic : String)
        #remove any MQTT shared subscription prefix
        filter_array = filter.sub(/^\$[^\/]+\/[^\/]+\//, "").split("/")
        topic_array = topic.split("/")
        length = filter_array.size

        # Normalise the strings
        filter_array.shift if filter_array[0].empty?
        topic_array.shift if topic_array[0].empty?

        filter_array.each_with_index do |left, index|
          right = topic_array[index]?

          return (topic_array.size >= (length - 1)) if left == "#"
          return false if left != "+" && left != right
        end

        topic_array.size == length
      end
    end # Client
  end   # V3
end     # MQTT
