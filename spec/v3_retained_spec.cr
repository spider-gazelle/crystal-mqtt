require "./spec_helper"

module MQTT::V3
  describe "retained messages" do
    it "sets the retain flag on the wire" do
      with_client do |client, broker|
        client.publish("state/device", "on", retain: true)

        published = broker.transport.sent_packets(Publish, MQTT::RequestType::Publish)
        published.size.should eq 1
        published.first.retain.should be_true
        published.first.topic.should eq "state/device"
      end
    end

    it "does not set the retain flag by default" do
      with_client do |client, broker|
        client.publish("state/device", "on")
        broker.transport.sent_packets(Publish, MQTT::RequestType::Publish).first.retain.should be_false
      end
    end

    # MQTT-3.3.1-5, the broker keeps the last retained message per topic and
    # delivers it to anyone who subscribes afterwards
    it "receives a retained message on subscribe" do
      with_client do |client, broker|
        client.publish("state/device", "on", retain: true)
        broker.retained.keys.should eq ["state/device"]

        received = [] of String
        client.subscribe("state/#") { |topic, payload| received << "#{topic}=#{String.new(payload)}"; nil }

        eventually { received.size == 1 }
        received.should eq ["state/device=on"]
      end
    end

    # MQTT-3.3.1-8, a subscriber has to be able to tell stored state from a
    # live update, which is what the retained flag is for
    it "flags a message delivered because of a new subscription as retained" do
      with_client do |client, broker|
        client.publish("state/device", "on", retain: true)

        seen = [] of Tuple(String, Bool)
        client.subscribe("state/#") { |topic, _, retained| seen << {topic, retained}; nil }
        eventually { seen.size == 1 }
        seen.should eq [{"state/device", true}]

        # a live publish afterwards must not be flagged
        broker.publish("state/device", "off")
        eventually { seen.size == 2 }
        seen[1].should eq({"state/device", false})
      end
    end

    it "keeps the two parameter callback working" do
      with_client do |client, _broker|
        client.publish("state/device", "on", retain: true)

        payloads = [] of String
        client.subscribe("state/#") { |_, payload| payloads << String.new(payload); nil }
        eventually { payloads.size == 1 }
        payloads.should eq ["on"]
      end
    end

    # MQTT-3.3.1-6, a retained publish replaces whatever was held before
    it "replaces the retained message for a topic" do
      with_client do |client, _broker|
        client.publish("state/device", "on", retain: true)
        client.publish("state/device", "off", retain: true)

        received = [] of String
        client.subscribe("state/device") { |_, payload| received << String.new(payload); nil }

        eventually { received.size == 1 }
        received.should eq ["off"]
      end
    end

    # MQTT-3.3.1-7, a zero length retained publish clears the topic
    it "clears a retained message with an empty payload" do
      with_client do |client, broker|
        client.publish("state/device", "on", retain: true)
        broker.retained.should_not be_empty

        client.publish("state/device", "", retain: true)
        broker.retained.should be_empty

        received = [] of String
        client.subscribe("state/device") { |_, payload| received << String.new(payload); nil }
        sleep 30.milliseconds
        received.should be_empty
      end
    end

    it "delivers a retained message per matching topic" do
      with_client do |client, _broker|
        client.publish("state/a", "1", retain: true)
        client.publish("state/b", "2", retain: true)
        client.publish("other/c", "3", retain: true)

        received = [] of String
        client.subscribe("state/+") { |topic, payload| received << "#{topic}=#{String.new(payload)}"; nil }

        eventually { received.size == 2 }
        received.sort.should eq ["state/a=1", "state/b=2"]
      end
    end

    # the retained store is broker side, so it has to outlive the connection
    it "survives a reconnect and is redelivered when subscriptions replay" do
      broker = FakeBroker.new
      client = broker.reconnecting_client(
        reconnect: MQTT::Reconnect.new(initial_delay: 5.milliseconds, max_delay: 10.milliseconds)
      )
      client.connect
      client.publish("state/device", "on", retain: true)

      seen = [] of Tuple(String, Bool)
      client.subscribe("state/#") { |_, payload, retained| seen << {String.new(payload), retained}; nil }
      eventually { seen.size == 1 }

      broker.drop!
      # the replayed SUBSCRIBE must pull the retained message down again
      eventually(5.seconds) { seen.size == 2 }
      seen.should eq [{"on", true}, {"on", true}]

      client.disconnect
    end
  end
end
