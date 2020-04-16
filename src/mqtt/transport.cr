require "../mqtt"
require "tokenizer"

module MQTT
  abstract class Transport
    abstract def close! : Nil
    abstract def closed? : Bool
    abstract def send(message) : Nil

    @on_tokenize : Proc(IO::Memory, Int32)?
    @on_message : Proc(Bytes, Nil)?
    @on_close : Proc(Nil)?
    getter error : ::Exception? = nil

    def on_message(&on_message : Bytes -> ) : Nil
      @on_message = on_message
    end

    def on_close(&on_close : -> )
      @on_close = on_close
    end

    def on_tokenize(&on_tokenize : IO::Memory -> Int32)
      @on_tokenize = on_tokenize
    end

    @tokenizer : Tokenizer

    def initialize
      @tokenizer = Tokenizer::Abstract.new { |buffer| @on_tokenize.try &.call(buffer) || -1 }
    end
  end
end

require "./transport/*"
