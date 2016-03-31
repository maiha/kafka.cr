class Kafka
  module Request
  end

  module Response
  end

  # An entry point to produce message, which contains topic and partition
  record Entry,
    topic : String,
    partition : Int32

  # A message itself to produce
  record Data,
    body : Slice(UInt8)

  struct Data
    def initialize(body : String)
      initialize(body.to_slice)
    end
  end
  
  # A message index to consume it, which contains topic and partition and offset
  record Index,
    topic : String,
    partition : Int32,
    offset : Int64 do

    def inspect(io : IO)
      if offset == -1
        io << %{"%s[%s]"} % [topic, partition]
      else
        io << %{"%s[%s]#%s"} % [topic, partition, offset]
      end
    end
  end

  record Value,
    binary : Slice(UInt8) do

    def string
      String.new(binary)
    end

    def inspect(io : IO)
      io << string.inspect
    rescue
      io << binary.inspect
    end
  end

  record Message,
    index : Kafka::Index,
    value : Kafka::Value do

    def inspect(io : IO)
      io << "Kafka::Message(" << index << ", " << value << ")"
    end
  end

  class MessageNotFound < Exception
    getter! index

    def initialize(@index : Kafka::Index)
      super("message not found: #{@index}")
    end
  end

  record MetadataInfo,
    brokers : Array(Broker),
    topics : Array(TopicInfo | TopicError)
  
  record TopicInfo,
    name : String,
    partition : Int32,
    leader : Int32,
    replicas : Array(Int32),
    isrs : Array(Int32)

  record TopicError,
    name : String,
    partition : Int32,
    code : Int16,
    error : String
  
  module OffsetsReader
    abstract def offsets : Array(Int64)

    def count
      return 0 if offsets.empty?
      first = offsets.first.not_nil!
      last = offsets.last.not_nil!
      return [first - last, 0].max
    end

    def offset
      return 0_i64 if offsets.empty?
      return offsets.first.not_nil!
    end
  end

  record Offset,
    index : Kafka::Index,
    offsets : Array(Int64) do

    include OffsetsReader

    def inspect(io : IO)
      io << "Kafka::Offset(" << index << ", count=" << count << ", offsets=" << offsets << ")"
    end
  end

  class OffsetNotFound < Exception
    getter! index

    def initialize(@index : Kafka::Index, msg : String = "offset not found")
      super("#{msg}: #{@index}")
    end
  end
end
