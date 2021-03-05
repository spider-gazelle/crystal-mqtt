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
  end # describe MQTT::V3
end   # MQTT::V3
