require "../transport"
require "openssl"

module MQTT
  class Transport::TCP < Transport
    def initialize(
      host,
      port = MQTT::DEFAULT_PORT,
      tls_context : OpenSSL::SSL::Context::Client? = nil,
      dns_timeout : Int32 = 10,
      connect_timeout : Int32 = 10,
      read_timeout : Int32? = nil,
      write_timeout : Int32? = nil,
    )
      super()

      # Connect to the server
      @socket = socket = TCPSocket.new(host, port, dns_timeout, connect_timeout)
      socket.tcp_nodelay = true
      socket.read_timeout = read_timeout.seconds if read_timeout
      socket.write_timeout = write_timeout.seconds if write_timeout

      if tls = tls_context
        # Sync true so TLS negotiation works
        socket.sync = true
        @socket = OpenSSL::SSL::Socket::Client.new(socket, context: tls, sync_close: true, hostname: host)
      else
        # only safe to buffer when we own the socket directly, a TLS wrapper
        # needs the underlying socket to write through on flush
        socket.sync = false
      end
    end

    def start : Nil
      start_dispatch { process! }
    end

    def close! : Nil
      @closing = true
      @socket.close
    end

    # tracks a deliberate shutdown so the IO::Error it provokes isn't
    # reported as a transport failure
    @closing : Bool = false

    def closed? : Bool
      !!@socket.closed?
    end

    def send(message) : Nil
      @socket.write_bytes(message)
      @socket.flush
    rescue error : IO::Error
      @socket.close
      raise error
    end

    @socket : IO

    # Incoming messages are read here and queued for ordered dispatch
    protected def process!
      Log.debug { "Processing incoming TCP messages..." }
      failure = nil

      raw_data = Bytes.new(2048)
      socket = @socket
      while !socket.closed?
        bytes_read = socket.read(raw_data)
        break if bytes_read == 0 # IO was closed

        process_incoming(raw_data[0, bytes_read])
      end
    rescue error
      # an IO::Error here is still a failure worth surfacing, callers need to be
      # able to tell a graceful disconnect from a broken pipe. The exception
      # provoked by our own `close!` is the one case that isn't a failure
      failure = error unless @closing
    ensure
      finish_processing(failure)
    end
  end
end
