require "../transport"
require "openssl"

module MQTT
  class Transport::TCP < Transport
    # NOTE:: the socket is not opened here. Connecting is deferred to `start`,
    # which the client calls once it is ready to consume data
    def initialize(
      @host : String,
      @port : Int32 = MQTT::DEFAULT_PORT,
      @tls_context : OpenSSL::SSL::Context::Client? = nil,
      @dns_timeout : Int32 = 10,
      @connect_timeout : Int32 = 10,
      @read_timeout : Int32? = nil,
      @write_timeout : Int32? = nil,
    )
      super()
    end

    getter host : String
    getter port : Int32

    @socket : IO? = nil

    # tracks a deliberate shutdown so the IO::Error it provokes isn't
    # reported as a transport failure
    @closing : Bool = false

    def start : Nil
      open_socket
      start_dispatch { process! }
    end

    private def open_socket : Nil
      socket = TCPSocket.new(@host, @port, @dns_timeout, @connect_timeout)
      socket.tcp_nodelay = true
      if read_timeout = @read_timeout
        socket.read_timeout = read_timeout.seconds
      end
      if write_timeout = @write_timeout
        socket.write_timeout = write_timeout.seconds
      end

      if tls = @tls_context
        # Sync true so TLS negotiation works
        socket.sync = true
        @socket = OpenSSL::SSL::Socket::Client.new(socket, context: tls, sync_close: true, hostname: @host)
      else
        # only safe to buffer when we own the socket directly, a TLS wrapper
        # needs the underlying socket to write through on flush
        socket.sync = false
        @socket = socket
      end
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
      socket.write_bytes(message)
      socket.flush
    rescue error : IO::Error
      @socket.try &.close
      raise error
    end

    # Incoming messages are read here and queued for ordered dispatch
    protected def process!
      Log.debug { "Processing incoming TCP messages..." }
      failure = nil
      socket = @socket

      raw_data = Bytes.new(2048)
      while socket && !socket.closed?
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
