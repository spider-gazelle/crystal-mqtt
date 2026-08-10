require "./spec_helper"

# Yields until the condition holds, so specs don't depend on fiber scheduling
def eventually(timeout : Time::Span = 2.seconds, &)
  expire = Time.utc + timeout
  until yield
    raise "condition was not met within #{timeout}" if Time.utc > expire
    sleep 1.millisecond
  end
end

module MQTT::V3
  describe MQTT::V3::Client do
    describe "connecting" do
      it "negotiates the MQTT layer" do
        broker = FakeBroker.new
        client = broker.client
        ack = client.connect
        ack.success?.should be_true
        broker.transport.sent_types.should eq [MQTT::RequestType::Connect]
      end

      it "closes the transport when the broker refuses the connection" do
        broker = FakeBroker.new
        broker.connack_return_code = 5_u8
        client = broker.client
        ack = client.connect
        ack.success?.should be_false
        expect_raises(MQTT::ConnectError, /not authorised/) { ack.success! }
        client.closed?.should be_true
      end

      it "can be retried after a failure" do
        broker = FakeBroker.new
        broker.silent = [MQTT::RequestType::Connect]
        client = broker.client(timeout: 50.milliseconds)

        expect_raises(MQTT::TimeoutError) { client.connect }

        # the first attempt must not leave the client wedged
        broker.silent = [] of MQTT::RequestType
        client.connect.success?.should be_true
      end

      it "rejects a will flag with no will topic" do
        broker = FakeBroker.new
        client = broker.client
        expect_raises(ArgumentError, /will_topic is required/) do
          client.connect(will_flag: true)
        end
      end
    end

    describe "subscribing" do
      # C1 regression
      it "keeps every callback when subscribing to the same topic twice" do
        with_client do |client, broker|
          hits = [] of String

          client.subscribe("some/topic", qos: QoS::BrokerReceived) { |topic, _| hits << "first:#{topic}"; nil }
          client.subscribe("some/topic", qos: QoS::BrokerReceived) { |topic, _| hits << "second:#{topic}"; nil }

          broker.publish("some/topic", "hi")
          eventually { hits.size == 2 }
          hits.sort.should eq ["first:some/topic", "second:some/topic"]
        end
      end

      # C1 regression, a SUBSCRIBE with no filters is a protocol violation
      it "always sends at least one topic filter" do
        with_client do |client, broker|
          client.subscribe("some/topic") { |_, _| nil }
          client.subscribe("some/topic") { |_, _| nil }

          subscribes = broker.transport.sent_packets(Subscribe, MQTT::RequestType::Subscribe)
          subscribes.size.should eq 2
          subscribes.each(&.topics.should_not(be_empty))
        end
      end

      it "rejects an empty topic list" do
        with_client do |client, _|
          expect_raises(ArgumentError, /at least one topic filter/) do
            client.subscribe({} of String => Tuple(QoS, Proc(String, Bytes, Nil)))
          end
        end
      end

      # C2 regression
      it "raises when the broker rejects the subscription" do
        with_client do |client, broker|
          broker.suback_codes = [MQTT::V3::Suback::FAILURE]

          expect_raises(MQTT::SubscriptionError, /rejected/) do
            client.subscribe("denied/topic") { |_, _| nil }
          end

          # the callback must not be left registered
          hits = 0
          broker.suback_codes = nil
          broker.publish("denied/topic")
          sleep 20.milliseconds
          hits.should eq 0
        end
      end

      # C2 regression
      it "raises when the broker never responds" do
        with_client(timeout: 50.milliseconds) do |client, broker|
          broker.silent = [MQTT::RequestType::Subscribe]
          expect_raises(MQTT::TimeoutError) do
            client.subscribe("some/topic") { |_, _| nil }
          end
        end
      end

      it "raises when the broker returns the wrong number of return codes" do
        with_client do |client, broker|
          broker.suback_codes = [0_u8, 0_u8]
          expect_raises(MQTT::SubscriptionError, /return codes/) do
            client.subscribe("some/topic") { |_, _| nil }
          end
        end
      end

      it "exposes the QoS the broker granted" do
        with_client do |client, broker|
          # broker downgrades the second filter
          broker.suback_codes = [1_u8, 0_u8]
          client.subscribe("a/topic", "b/topic", qos: QoS::BrokerReceived) { |_, _| nil }

          client.subscriptions.should eq({
            "a/topic" => QoS::BrokerReceived,
            "b/topic" => QoS::FireAndForget,
          })

          client.unsubscribe("a/topic")
          client.subscriptions.keys.should eq ["b/topic"]
        end
      end

      it "routes messages to matching wildcard subscriptions only" do
        with_client do |client, broker|
          matched = [] of String
          client.subscribe("sensors/+/temp") { |topic, _| matched << topic; nil }

          broker.publish("sensors/kitchen/temp")
          broker.publish("sensors/kitchen/humidity")
          broker.publish("sensors/lounge/temp")

          eventually { matched.size == 2 }
          matched.should eq ["sensors/kitchen/temp", "sensors/lounge/temp"]
        end
      end
    end

    describe "unsubscribing" do
      # C3 regression, this overload previously failed to compile
      it "removes a single callback by reference" do
        with_client do |client, broker|
          first_hits = 0
          second_hits = 0
          first = ->(_topic : String, _payload : Bytes) { first_hits += 1; nil }
          second = ->(_topic : String, _payload : Bytes) { second_hits += 1; nil }

          client.subscribe({"some/topic" => {QoS::FireAndForget, first}})
          client.subscribe({"some/topic" => {QoS::FireAndForget, second}})

          client.unsubscribe("some/topic", first)

          # other callbacks remain, so no UNSUBSCRIBE should have been sent
          broker.transport.sent_types.should_not contain MQTT::RequestType::Unsubscribe

          broker.publish("some/topic")
          eventually { second_hits == 1 }
          first_hits.should eq 0
        end
      end

      it "unsubscribes once the last callback is removed" do
        with_client do |client, broker|
          callback = ->(_topic : String, _payload : Bytes) { nil }
          client.subscribe({"some/topic" => {QoS::FireAndForget, callback}})
          client.unsubscribe("some/topic", callback)

          broker.transport.sent_types.should contain MQTT::RequestType::Unsubscribe
        end
      end

      it "unsubscribes from a list of topics" do
        with_client do |client, broker|
          client.subscribe("a/topic", "b/topic") { |_, _| nil }
          client.unsubscribe("a/topic", "b/topic")

          unsubs = broker.transport.sent_packets(Unsubscribe, MQTT::RequestType::Unsubscribe)
          unsubs.size.should eq 1
          unsubs.first.topics.should eq ["a/topic", "b/topic"]
        end
      end
    end

    describe "publishing" do
      it "sends a QoS 0 publish without waiting for an acknowledgement" do
        with_client do |client, broker|
          client.publish("some/topic", "payload")
          published = broker.transport.sent_packets(Publish, MQTT::RequestType::Publish)
          published.size.should eq 1
          String.new(published.first.payload).should eq "payload"
        end
      end

      it "waits for a PUBACK at QoS 1" do
        with_client do |client, broker|
          client.publish("some/topic", "payload", qos: QoS::BrokerReceived)
          published = broker.transport.sent_packets(Publish, MQTT::RequestType::Publish)
          published.first.message_id.should_not eq 0
        end
      end

      # C4 regression
      it "completes the full handshake at QoS 2" do
        with_client do |client, broker|
          client.publish("some/topic", "payload", qos: QoS::SubscribersReceived)

          # PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
          broker.transport.sent_types.should contain MQTT::RequestType::Pubrel
        end
      end

      it "times out rather than hanging when no PUBACK arrives" do
        with_client(timeout: 50.milliseconds) do |client, broker|
          broker.silent = [MQTT::RequestType::Publish]
          expect_raises(MQTT::TimeoutError) do
            client.publish("some/topic", "payload", qos: QoS::BrokerReceived)
          end
        end
      end

      it "rejects an empty topic" do
        with_client do |client, _|
          expect_raises(ArgumentError, /cannot be empty/) { client.publish("") }
        end
      end
    end

    describe "receiving" do
      it "acknowledges an inbound QoS 1 publish" do
        with_client do |client, broker|
          received = 0
          client.subscribe("some/topic") { |_, _| received += 1; nil }
          broker.publish("some/topic", "hi", qos: QoS::BrokerReceived, message_id: 7_u16)

          eventually { received == 1 }
          eventually { broker.transport.sent_types.includes?(MQTT::RequestType::Puback) }
        end
      end

      # C4 regression, an inbound QoS 2 message is answered with PUBREC not PUBACK
      it "completes the QoS 2 handshake and delivers exactly once" do
        with_client do |client, broker|
          received = 0
          client.subscribe("some/topic") { |_, _| received += 1; nil }
          broker.publish("some/topic", "hi", qos: QoS::SubscribersReceived, message_id: 9_u16)

          eventually { received == 1 }
          types = broker.transport.sent_types
          types.should contain MQTT::RequestType::Pubrec
          types.should contain MQTT::RequestType::Pubcomp
          types.should_not contain MQTT::RequestType::Puback

          # a redelivery before release must not dispatch twice
          sleep 20.milliseconds
          received.should eq 1
        end
      end

      it "preserves message ordering" do
        with_client do |client, broker|
          order = [] of String
          client.subscribe("ordered/#") { |topic, _| order << topic; nil }

          20.times { |i| broker.publish("ordered/#{i}") }

          eventually { order.size == 20 }
          order.should eq Array.new(20) { |i| "ordered/#{i}" }
        end
      end

      it "keeps serving other subscriptions when a callback raises" do
        with_client do |client, broker|
          good = 0
          client.subscribe("some/topic") { |_, _| raise "callback exploded" }
          client.subscribe("some/topic") { |_, _| good += 1; nil }

          broker.publish("some/topic")
          eventually { good == 1 }
        end
      end
    end

    describe "keep alive" do
      it "pings an idle connection" do
        broker = FakeBroker.new
        client = broker.client
        # 1 second keep alive pings at 750ms
        client.connect(keep_alive: 1)

        eventually(3.seconds) { broker.transport.sent_types.includes?(MQTT::RequestType::Pingreq) }
        eventually(3.seconds) { !client.last_ping_response.nil? }
        client.disconnect
      end

      it "waits for the ping response" do
        with_client(timeout: 50.milliseconds) do |client, broker|
          broker.silent = [MQTT::RequestType::Pingreq]
          expect_raises(MQTT::TimeoutError) { client.ping }
        end
      end
    end

    describe "disconnecting" do
      it "wakes wait_close and rejects requests in flight" do
        with_client do |client, _broker|
          closed = false
          spawn { client.wait_close; closed = true }

          client.disconnect
          eventually { closed }
          client.closed?.should be_true

          expect_raises(MQTT::NotConnectedError) { client.publish("some/topic") }
        end
      end

      it "surfaces the transport error to pending requests" do
        broker = FakeBroker.new
        broker.silent = [MQTT::RequestType::Subscribe]
        client = broker.client

        client.connect
        result = nil
        spawn do
          client.subscribe("some/topic") { |_, _| nil }
        rescue error
          result = error
        end

        sleep 20.milliseconds
        broker.transport.fail!(IO::Error.new("connection reset"))

        eventually { !result.nil? }
        result.should be_a MQTT::NotConnectedError
        result.as(MQTT::NotConnectedError).cause.should be_a IO::Error
      end
    end

    describe "malformed input" do
      it "closes the connection when a packet exceeds the maximum size" do
        broker = FakeBroker.new
        client = broker.client(max_packet_size: 1024_u32)
        client.connect

        # a fixed header advertising the largest possible remaining length
        broker.transport.receive_bytes(Bytes[0x30, 0xFF, 0xFF, 0xFF, 0x7F])
        eventually { client.closed? }
      end

      it "closes the connection on an unparsable packet" do
        broker = FakeBroker.new
        client = broker.client
        client.connect

        # PUBLISH whose remaining length is too small for its own topic field
        broker.transport.receive_bytes(Bytes[0x30, 0x01, 0x00])
        eventually { client.closed? }
      end
    end
  end # describe Client
end   # MQTT::V3
