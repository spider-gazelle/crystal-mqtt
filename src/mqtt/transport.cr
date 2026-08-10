require "./base"
require "tokenizer"

module MQTT
  abstract class Transport
    abstract def close! : Nil
    abstract def closed? : Bool
    abstract def send(message) : Nil

    # Begins consuming data from the remote host.
    # Called by the client once its callbacks have been configured so that no
    # data can arrive before there is something able to process it
    abstract def start : Nil

    @on_tokenize : Proc(IO::Memory, Int32)?
    @on_message : Proc(Bytes, Nil)?
    @on_close : Proc(Nil)?
    getter error : ::Exception? = nil

    def on_message(&on_message : Bytes ->) : Nil
      @on_message = on_message
    end

    def on_close(&on_close : ->)
      @on_close = on_close
    end

    def on_tokenize(&on_tokenize : IO::Memory -> Int32)
      @on_tokenize = on_tokenize
    end

    @tokenizer : Tokenizer
    @started : Bool = false
    @start_lock = Mutex.new

    # Bounded so a fast remote host can't queue an unlimited number of messages
    @inbound = ::Channel(Bytes).new(64)

    def initialize
      @tokenizer = Tokenizer::Abstract.new { |buffer| @on_tokenize.try &.call(buffer) || -1 }
    end

    # Idempotent, so a client that is re-created against the same transport
    # won't spawn a second set of fibers
    protected def start_dispatch(&read_loop : ->) : Nil
      @start_lock.synchronize do
        return if @started
        @started = true
      end

      spawn(name: "mqtt-dispatch") { dispatch_messages! }
      spawn(name: "mqtt-transport") { read_loop.call }
    end

    # Tokenizes incoming data and queues whole packets for dispatch.
    #
    # NOTE:: this deliberately does not `spawn` per message. MQTT guarantees
    # ordered delivery within a QoS level and a fiber per message discards that
    # ordering, as well as allowing unbounded fiber growth under load
    protected def process_incoming(data : Bytes) : Nil
      @tokenizer.extract(data).each do |bytes|
        @inbound.send(bytes)
      end
    rescue ::Channel::ClosedError
      # transport shut down while we were queuing, nothing left to do
    end

    # Signals that no further data will arrive.
    # `on_close` fires once the already queued messages have been processed
    protected def finish_processing(@error : ::Exception? = @error) : Nil
      @inbound.close
    end

    private def dispatch_messages! : Nil
      while bytes = @inbound.receive?
        begin
          @on_message.try &.call(bytes)
        rescue error
          Log.error(exception: error) { "error dispatching message" }
        end
      end
    ensure
      # guarantees `on_close` fires exactly once, and only after every
      # message received before the disconnect has been handled
      @on_close.try &.call
    end
  end
end

require "./transport/*"
