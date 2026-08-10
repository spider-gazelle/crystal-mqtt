require "../transport"
require "http/web_socket"

module MQTT
  class Transport::Websocket < Transport
    # NOTE:: the socket is not opened here. Connecting is deferred to `start`,
    # which the client calls once it is ready to consume data
    def initialize(
      @host : String,
      @path : String,
      @port : Int32? = nil,
      @tls : HTTP::Client::TLSContext = nil,
      @headers : HTTP::Headers = HTTP::Headers.new,
    )
      super()
    end

    getter host : String
    getter path : String

    @socket : HTTP::WebSocket? = nil
    @closing : Bool = false

    def start : Nil
      socket = HTTP::WebSocket.new(@host, @path, @port, @tls, @headers)
      socket.on_binary { |data| process_incoming(data) }
      socket.on_message { |data| process_incoming(data.to_slice) }
      @socket = socket

      start_dispatch { process! }
    end

    def close! : Nil
      @closing = true
      @socket.try &.close
    end

    def closed? : Bool
      socket = @socket
      socket.nil? || socket.closed?
    end

    def send(message) : Nil
      socket = @socket || raise MQTT::NotConnectedError.new("transport has not been started")
      socket.send(message.to_slice)
    rescue error : IO::Error
      @socket.try &.close
      raise error
    end

    protected def process!
      failure = nil
      @socket.try &.run
    rescue error
      # previously this ran in a bare `spawn` and any failure was lost to an
      # unhandled fiber exception
      failure = error unless @closing
    ensure
      finish_processing(failure)
    end
  end
end
