require "../mqtt"

module MQTT
  # Controls how a client re-establishes a connection that dropped
  # unexpectedly. Delays back off exponentially and are capped at `max_delay`.
  struct Reconnect
    getter initial_delay : Time::Span
    getter max_delay : Time::Span

    # `nil` retries forever
    getter max_attempts : Int32?

    def initialize(
      @initial_delay : Time::Span = 1.second,
      @max_delay : Time::Span = 30.seconds,
      @max_attempts : Int32? = nil,
    )
      raise ArgumentError.new("initial_delay must be positive") unless @initial_delay.positive?
      raise ArgumentError.new("max_delay must be at least initial_delay") if @max_delay < @initial_delay
    end

    # `attempt` is 1 based
    def delay_for(attempt : Int32) : Time::Span
      return @initial_delay if attempt <= 1

      # doubling in nanoseconds, guarding against the shift overflowing on a
      # connection that has been failing for a very long time
      exponent = Math.min(attempt - 1, 32)
      scaled = @initial_delay.total_nanoseconds * (2_i64 ** exponent)
      return @max_delay if scaled > @max_delay.total_nanoseconds

      Time::Span.new(nanoseconds: scaled.to_i64)
    end

    def give_up?(attempt : Int32) : Bool
      if limit = @max_attempts
        attempt > limit
      else
        false
      end
    end
  end
end
