require "./pretty_binary"

module Utils::GuessBinary
  include Utils::PrettyBinary

  abstract class Guessed
    getter text, bytes

    def self.match?(bytes : Slice(UInt8))
      true
    end

    def initialize(@text : String, @bytes : Slice(UInt8))
    end

    def to_s
      text
    end
  end

  class Unknown < Guessed
    def initialize(bytes)
      super("(unknown) #{bytes.inspect}", bytes)
    end
  end

  class Null < Guessed
    def self.match?(bytes : Slice(UInt8))
      bytes.empty?
    end

    def initialize(bytes)
      super("(null) #{bytes.inspect}", bytes)
    end
  end

  class Text < Guessed
    def self.match?(bytes : Slice(UInt8))
      s = String.new(bytes, "UTF-8")
      return false if !s.valid_encoding?
      return false if s.inspect =~ /\\u(\d){4}/
      return true
    rescue
      return false
    end

    def initialize(bytes)
      text = String.new(bytes)
      super("(text) #{text.inspect}", bytes)
    end
  end

  class Msgpack < Guessed
    def self.match?(bytes : Slice(UInt8))
      128 <= bytes.first.not_nil! <= 159
    end

    def initialize(bytes)
      unpacker = MessagePack::Unpacker.new(bytes)
      value = unpacker.read_value.inspect
      super("(msgpack) #{value}", bytes)
    end
  end

  def guess_binary(bytes : Slice(UInt8)) : Guessed
    [Null, Msgpack, Text].each do |decoder|
      return decoder.new(bytes) if decoder.match?(bytes)
    end
    raise "not match"
  rescue
    return Unknown.new(bytes)
  end
end
