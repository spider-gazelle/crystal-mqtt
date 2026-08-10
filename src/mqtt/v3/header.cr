require "../header"

module MQTT
  module V3
    # The fixed header is byte identical in 3.1.1 and 5.0, so it lives in
    # `MQTT::Header`. Kept here so existing references keep resolving
    alias Header = ::MQTT::Header
  end # V3
end   # MQTT
