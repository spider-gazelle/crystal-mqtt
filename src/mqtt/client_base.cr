require "./transport"
require "./pending"
require "./reconnect"
require "./header"
require "./topic"
require "mutex"

module MQTT
  # The version agnostic half of an MQTT client: transport lifecycle, framing,
  # the request pipeline, packet identifier allocation, the registry of
  # responses we're waiting on, keep alive and reconnection.
  #
  # Everything that depends on the wire format of a particular protocol version
  # — building packets, parsing them, and what an acknowledgement means — is
  # left to the subclass.
  abstract class ClientBase
    # How long to wait for a broker response before giving up
    DEFAULT_TIMEOUT = 30.seconds

    # Applied to any request that isn't given an explicit timeout.
    # Set to `nil` to wait indefinitely
    property timeout : Time::Span?

    # Largest packet we're willing to buffer from the broker
    getter max_packet_size : UInt32

    getter last_ping_response : Time? = nil

    @message_lock = Mutex.new
    @message_id = 0_u16

    # Responses we're waiting on, keyed by the packet type we expect and the
    # packet identifier it will carry.
    #
    # NOTE:: typed as `Pending(Header)` because every packet class descends from
    # `Header`, which keeps this registry version agnostic. Subclasses cast the
    # result to the concrete type they asked for
    @waiting = {} of Tuple(RequestType, UInt16) => Pending(Header)
    @waiting_connect : Pending(Header)? = nil

    # PINGRESP carries no identifier, so these are matched in order
    @waiting_ping = [] of Pending(Header)

    # Monotonic timestamp of the last packet sent or received, used to decide
    # whether a keep alive ping is required
    @last_activity : MQTT::Monotonic = MQTT.monotonic

    @transport : Transport

    # Builds a fresh transport when reconnecting. A socket cannot be reopened,
    # so reconnection needs a factory rather than the transport itself
    @transport_factory : Proc(Transport)?
    @reconnect : MQTT::Reconnect?

    # set once the client is finished for good, so a deliberate disconnect
    # isn't mistaken for a dropped connection
    @terminated : Bool = false
    @reconnecting : Bool = false

    # whether a session has been established that a reconnect could restore
    @can_resume : Bool = false

    # Drives the client over a single transport, with no reconnection
    def initialize(
      transport : Transport,
      @timeout : Time::Span? = DEFAULT_TIMEOUT,
      @max_packet_size : UInt32 = MQTT::DEFAULT_MAX_PACKET_SIZE,
    )
      @transport = transport
      @wait_close = Channel(Nil).new
      spawn(name: "mqtt-requests") { process_requests! }
    end

    # Drives the client over transports produced by *factory*, re-establishing
    # the connection whenever it drops
    def initialize(
      @timeout : Time::Span?,
      @max_packet_size : UInt32,
      factory : Proc(Transport),
      reconnect : MQTT::Reconnect,
    )
      @transport_factory = factory
      @reconnect = reconnect
      @transport = factory.call
      @wait_close = Channel(Nil).new
      spawn(name: "mqtt-requests") { process_requests! }
    end

    # Subclasses call this as the last statement of their constructor. The
    # transport can deliver a packet the moment it starts, so nothing may run
    # before the subclass has finished initialising its own state
    protected def start_transport : Nil
      attach(@transport)
    end

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

    def closed?
      @transport.closed?
    end

    def terminated? : Bool
      @terminated
    end

    # Returns once the MQTT connection has terminated
    def wait_close : Nil
      @wait_close.receive?
    end

    # ---- framing -----------------------------------------------------------

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

    # Handles a decoded packet. Implemented per protocol version
    abstract def parse_message(io)

    # ---- connection lifecycle ----------------------------------------------

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
      waiting = [] of Pending(Header)
      pings = [] of Pending(Header)

      @message_lock.synchronize do
        connecting = @waiting_connect
        @waiting_connect = nil
        waiting = @waiting.values
        @waiting.clear
        pings = @waiting_ping.dup
        @waiting_ping.clear
        reset_connection_state
      end

      connecting.try &.reject(failure)
      waiting.each &.reject(failure)
      pings.each &.reject(failure)

      if reconnect_wanted?
        spawn(name: "mqtt-reconnect") { reconnect! }
      else
        terminate!
      end
    end

    # Discards any per-connection state the subclass is holding.
    # NOTE:: called with `@message_lock` held
    protected abstract def reset_connection_state : Nil

    # Whether the connection dropped in a way we should recover from. A
    # deliberate `disconnect` is not one, and neither is a client that never
    # got a transport factory.
    # Claims the reconnect, so a transport closing mid-retry can't start a
    # second loop
    private def reconnect_wanted? : Bool
      return false if @terminated || @transport_factory.nil?
      return false unless reconnect_permitted?

      @message_lock.synchronize do
        # nothing to re-establish until a connection has been made once
        next false if @reconnecting || !@can_resume
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

    # Lets a subclass veto a reconnect, for example when the broker gave a
    # reason that says not to come back
    protected def reconnect_permitted? : Bool
      true
    end

    # Replays whatever is needed to put the session back the way it was.
    # Implemented per protocol version
    protected abstract def resume_session : Nil

    # Stops the request processor and wakes anything in `wait_close`
    protected def terminate! : Nil
      @terminated = true
      @processor.close
      @wait_close.close
    end

    # ---- request pipeline --------------------------------------------------

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

    # ---- packet identifiers and pending responses --------------------------

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
      @waiting.each_key { |key| return true if key[1] == id }
      false
    end

    # Registers interest in a response of *type* carrying *id*
    protected def expect(type : RequestType, id : UInt16) : Pending(Header)
      pending = Pending(Header).new
      @message_lock.synchronize { @waiting[{type, id}] = pending }
      pending
    end

    protected def forget(type : RequestType, id : UInt16) : Nil
      @message_lock.synchronize { @waiting.delete({type, id}) }
    end

    # Completes a waiting request. Returns false when nothing was expecting it
    protected def resolve(type : RequestType, id : UInt16, packet : Header) : Bool
      pending = @message_lock.synchronize { @waiting[{type, id}]? }
      return false unless pending
      pending.resolve(packet)
      true
    end

    # Allocates a packet identifier and registers interest in the response.
    # NOTE:: both happen under one lock, otherwise a second fiber can be handed
    # the same identifier before this one has claimed it
    protected def expect_next(type : RequestType) : Tuple(UInt16, Pending(Header))
      pending = Pending(Header).new
      message_id = @message_lock.synchronize do
        id = next_message_id
        @waiting[{type, id}] = pending
        id
      end
      {message_id, pending}
    end

    # Allocates a message id, registers the expected response and guarantees
    # the registration is cleaned up however the block exits
    protected def reserve_ack(expecting : RequestType, &)
      message_id, pending = expect_next(expecting)

      begin
        yield message_id, pending
      ensure
        @message_lock.synchronize { @waiting.delete({expecting, message_id}) }
      end

      message_id
    end

    # ---- keep alive --------------------------------------------------------

    # Sends whatever this protocol version uses for a liveness check, and waits
    # for the response
    abstract def ping(timeout : Time::Span?) : Nil

    # Sends a ping whenever the link has been idle, so the broker doesn't drop
    # us for exceeding the keep alive interval we negotiated
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
          ping(interval)
        rescue error
          Log.warn(exception: error) { "keep alive ping failed, closing connection" }
          @transport.close!
          break
        end
      end
    end
  end
end
