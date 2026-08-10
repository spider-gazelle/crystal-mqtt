require "../mqtt"

module MQTT
  # Does a topic name match a subscription filter?
  #
  # The wildcard rules are unchanged between 3.1.1 and 5.0, so both clients
  # share this. Based on https://github.com/ralphtheninja/mqtt-match
  def self.topic_matches?(filter : String, topic : String) : Bool
    filter_array = filter.split("/")
    # remove any MQTT shared subscription prefix
    # https://emqx.medium.com/introduction-to-mqtt-5-0-protocol-shared-subscription-4c23e7e0e3c1
    if filter_array.first? == "$share"
      filter_array = filter_array.size > 2 ? filter_array[2..] : [] of String
    end
    topic_array = topic.split("/")

    # Normalise the strings
    topic_array.shift if topic_array[0].empty?
    filter_array.shift if filter_array.first?.try(&.empty?)

    # MQTT-4.7.2-1: a wildcard at the first level must not match a topic
    # beginning with `$`, those are reserved for the broker
    if topic_array.first?.try(&.starts_with?('$'))
      leading = filter_array.first?
      return false if leading == "#" || leading == "+"
    end

    length = filter_array.size

    filter_array.each_with_index do |left, index|
      right = topic_array[index]?

      return (topic_array.size >= (length - 1)) if left == "#"
      return false if left != "+" && left != right
    end

    topic_array.size == length
  end
end
