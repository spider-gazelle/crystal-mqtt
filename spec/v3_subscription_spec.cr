require "./spec_helper"

module MQTT::V3
  describe MQTT::V3 do
    it "should match basic topics" do
      MQTT::V3::Client.topic_matches("my/favorite/topic", "my/favorite/topic").should be_true
      MQTT::V3::Client.topic_matches("my/favorite/topic", "my/favorite").should be_false
      MQTT::V3::Client.topic_matches("my/favorite/topic", "my/favorite/topic/etc").should be_false
    end

    it "should match topics with multi-level wildcards" do
      MQTT::V3::Client.topic_matches("my/favorite/topic/#", "my/favorite/topic/a").should be_true
      MQTT::V3::Client.topic_matches("my/favorite/topic/#", "my/favorite/topic/a/b").should be_true
    end

    it "should match topics with single-level wildcards" do
      MQTT::V3::Client.topic_matches("my/favorite/topic/+/here", "my/favorite/topic/is/here").should be_true
      MQTT::V3::Client.topic_matches("my/favorite/topic/+/here", "my/favorite/topic/is/not").should be_false
      MQTT::V3::Client.topic_matches("my/favorite/topic/+/here", "my/favorite/topic/here").should be_false
    end

    it "should match shared subscriptions" do
      MQTT::V3::Client.topic_matches("$share/groupid/my/favorite/topic/+/here", "my/favorite/topic/is/here").should be_true
    end

    it "should tolerate a malformed shared subscription prefix" do
      MQTT::V3::Client.topic_matches("$share", "my/topic").should be_false
      MQTT::V3::Client.topic_matches("$share/groupid", "my/topic").should be_false
    end

    # MQTT-4.7.2-1, wildcards must not match the broker's reserved topics
    it "should not match $ topics with a leading wildcard" do
      MQTT::V3::Client.topic_matches("#", "$SYS/broker/uptime").should be_false
      MQTT::V3::Client.topic_matches("+/broker/uptime", "$SYS/broker/uptime").should be_false
    end

    it "should match $ topics when the filter names them explicitly" do
      MQTT::V3::Client.topic_matches("$SYS/#", "$SYS/broker/uptime").should be_true
      MQTT::V3::Client.topic_matches("$SYS/+/uptime", "$SYS/broker/uptime").should be_true
    end

    it "should match a multi-level wildcard against its parent level" do
      MQTT::V3::Client.topic_matches("my/favorite/#", "my/favorite").should be_true
      MQTT::V3::Client.topic_matches("#", "my/favorite").should be_true
    end
  end # describe MQTT::V3
end   # MQTT::V3
