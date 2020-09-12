require "../transport"
require "http/web_socket"

module MQTT
  class Transport::Websocket < Transport
    def initialize(host : String, path : String, port = nil, tls : HTTP::Client::TLSContext = nil, headers = HTTP::Headers.new)
      super()

      # Connect to the server
      @socket = socket = HTTP::WebSocket.new(host, path, port, tls, headers)
      socket.on_close { @on_close.try &.call }
      socket.on_binary { |data| process(data) }
      socket.on_message { |data| process(data.to_slice) }
      spawn { socket.run }
    end

    def close! : Nil
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

    protected def process(data : Bytes)
      @tokenizer.extract(data).each do |bytes|
        spawn { @on_message.try &.call(bytes) }
      end
    end
  end
end
