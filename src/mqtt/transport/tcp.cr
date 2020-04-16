require "../transport"
require "openssl"

module MQTT
  class Transport::TCP < Transport
    def initialize(
      host,
      port = MQTT::DEFAULT_PORT,
      tls_context : OpenSSL::SSL::Context::Client? = nil,
      dns_timeout : Int32 = 10,
      connect_timeout : Int32 = 10
    )
      super()

      # Connect to the server
      @socket = socket = TCPSocket.new(host, port, dns_timeout, connect_timeout)
      socket.tcp_nodelay = true

      if tls = tls_context
        # Sync true so TLS negotiation works
        socket.sync = true
        @socket = OpenSSL::SSL::Socket::Client.new(socket, context: tls, sync_close: true, hostname: host)
      end
      socket.sync = false

      spawn { process! }
    end

    def close! : Nil
      @socket.close
    end

    def closed? : Bool
      !!@socket.closed?
    end

    def send(message) : Nil
      @socket.write_bytes(message)
      @socket.flush
    end

    @socket : IO

    # Incomming messages are processed here
    # The message is then processed on a new thread
    protected def process!
      Log.debug { "Processing incoming TCP messages..." }

      raw_data = Bytes.new(2048)
      if socket = @socket
        while !socket.closed?
          bytes_read = socket.read(raw_data)
          break if bytes_read == 0 # IO was closed

          @tokenizer.extract(raw_data[0, bytes_read]).each do |bytes|
            spawn { @on_message.try &.call(bytes) }
          end
        end
      end
    rescue IO::Error
    rescue error
      @error = error
    ensure
      @on_close.try &.call
    end
  end
end
