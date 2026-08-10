require "spec"
require "../src/mqtt"
require "../src/mqtt/v3/client"
require "./support/fake_broker"

# Yields until the condition holds, so specs don't depend on fiber scheduling
def eventually(timeout : Time::Span = 2.seconds, &)
  expire = Time.utc + timeout
  until yield
    raise "condition was not met within #{timeout}" if Time.utc > expire
    sleep 1.millisecond
  end
end

def combine(*args)
  io = IO::Memory.new
  args.each do |part|
    io.write(part.to_slice)
  end
  io.rewind
  io
end

def new_publish_packet
  packet = MQTT::V3::Publish.new
  packet.id = MQTT::RequestType::Publish
  packet
end

def new_connect_packet(version : MQTT::Version, client_id : String)
  packet = MQTT::V3::Connect.new
  packet.id = MQTT::RequestType::Connect
  case version
  when MQTT::Version::V31
    packet.version = MQTT::Version::V31
    packet.name = packet.version.connect_name
  when MQTT::Version::V311
    # This is the default
  when MQTT::Version::V5
    raise "currently unsupported"
  end
  packet.client_id = client_id
  packet.clean_start = true
  packet.keep_alive_seconds = 15
  packet
end

def new_connack_packet
  packet = MQTT::V3::Connack.new
  packet.id = MQTT::RequestType::Connack
  packet
end
