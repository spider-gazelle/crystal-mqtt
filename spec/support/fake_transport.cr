require "../../src/mqtt/v3/client"

# An in-memory transport, so the client can be driven without a socket.
#
# `sent` records every packet the client wrote, `receive_packet` feeds a packet
# back as though the broker had sent it.
class FakeTransport < MQTT::Transport
  getter sent = [] of Bytes

  @closed = false
  @close_lock = Mutex.new

  # there is no read loop, `receive_bytes` pushes data in directly
  def start : Nil
    start_dispatch { }
  end

  def close! : Nil
    should_finish = @close_lock.synchronize do
      next false if @closed
      @closed = true
    end
    finish_processing if should_finish
  end

  def closed? : Bool
    @closed
  end

  def send(message) : Nil
    raise IO::Error.new("transport closed") if @closed
    io = IO::Memory.new
    io.write_bytes(message)
    @sent << io.to_slice
    @on_send.try &.call(message.as(MQTT::V3::Header))
  end

  # Invoked with every packet the client transmits, this is where a fake broker
  # decides how to respond
  def on_send(&@on_send : MQTT::V3::Header ->)
  end

  @on_send : Proc(MQTT::V3::Header, Nil)?

  # Simulates the broker sending us a packet
  def receive_packet(packet) : Nil
    io = IO::Memory.new
    io.write_bytes(packet)
    receive_bytes(io.to_slice)
  end

  # Simulates raw bytes arriving, exercising the tokenizer
  def receive_bytes(bytes : Bytes) : Nil
    process_incoming(bytes)
  end

  # Simulates the remote host vanishing
  def fail!(error : ::Exception) : Nil
    @closed = true
    finish_processing(error)
  end

  # Decoded view of what the client transmitted
  def sent_packets(klass : T.class, type : MQTT::RequestType) : Array(T) forall T
    sent.compact_map do |bytes|
      io = IO::Memory.new(bytes)
      next unless MQTT.peek_type(io) == type
      io.read_bytes(klass)
    end
  end

  def sent_types : Array(MQTT::RequestType)
    sent.map { |bytes| MQTT.peek_type(IO::Memory.new(bytes)) }
  end
end
