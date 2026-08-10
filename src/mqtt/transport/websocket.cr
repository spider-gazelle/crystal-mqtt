require "../transport"
require "http/web_socket"

module MQTT
  class Transport::Websocket < Transport
    def initialize(host : String, path : String, port = nil, tls : HTTP::Client::TLSContext = nil, headers = HTTP::Headers.new)
      super()

      # Connect to the server
      @socket = socket = HTTP::WebSocket.new(host, path, port, tls, headers)
      socket.on_binary { |data| process_incoming(data) }
      socket.on_message { |data| process_incoming(data.to_slice) }
    end

    def start : Nil
      start_dispatch { process! }
    end

    def close! : Nil
      @closing = true
      @socket.close
    end

    def closed? : Bool
      !!@socket.closed?
    end

    def send(message) : Nil
      @socket.send(message.to_slice)
    rescue error : IO::Error
      @socket.close
      raise error
    end

    @socket : HTTP::WebSocket
    @closing : Bool = false

    protected def process!
      failure = nil
      @socket.run
    rescue error
      # previously this ran in a bare `spawn` and any failure was lost to an
      # unhandled fiber exception
      failure = error unless @closing
    ensure
      finish_processing(failure)
    end
  end
end
