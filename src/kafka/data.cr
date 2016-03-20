class Kafka
  module Request
  end

  module Response
  end

  record Index,
    topic : String,
    partition : Int32,
    offset : Int64 do

    def inspect(io : IO)
      io << %{"%s[%s]#%s"} % [topic, partition, offset]
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
    def initialize(@index : Kafka::Index)
      super("message not found: #{@index}")
    end
  end
end
