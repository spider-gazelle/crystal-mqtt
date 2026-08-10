require "./base"

module MQTT
  # A single-shot slot for a response we're expecting from the broker.
  #
  # This replaces the promises that used to carry request/response plumbing.
  # `Promise::DeferredPromise#get` spawns a fiber and allocates a channel on
  # every call, and `Promise.timeout` costs another fiber and channel per
  # request. More importantly a promise runs its callbacks inline on whichever
  # fiber resolves it, which made it unsafe to resolve one while holding a lock.
  #
  # Completion is signalled by closing a channel, which wakes every waiter
  # rather than handing the value to whoever happens to receive first.
  class Pending(T)
    def initialize
      @signal = ::Channel(Nil).new
      @lock = Mutex.new
    end

    @result : (T | ::Exception)?

    def completed? : Bool
      !@result.nil?
    end

    def resolve(value : T) : Nil
      complete(value)
    end

    def reject(error : ::Exception) : Nil
      complete(error)
    end

    private def complete(value : T | ::Exception) : Nil
      @lock.synchronize do
        # first completion wins, later ones are ignored
        return if @result
        @result = value
      end

      # closing broadcasts, `receive?` then returns immediately for every waiter
      @signal.close
    end

    # Blocks the calling fiber until the response arrives.
    # Raises `MQTT::TimeoutError` if `timeout` elapses first.
    def get(timeout : Time::Span? = nil, description : String = "response") : T
      if timeout
        select
        when @signal.receive?
          # completed
        when ::timeout(timeout)
          raise MQTT::TimeoutError.new("timeout waiting for #{description} after #{timeout}")
        end
      else
        @signal.receive?
      end

      case result = @result
      in ::Exception
        raise result
      in T
        result
      in Nil
        raise MQTT::Error.new("#{description} completed without a result")
      end
    end
  end
end
