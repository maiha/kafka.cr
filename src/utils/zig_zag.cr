module ZigZag
  class Var(T)
    getter value
    getter read_bytes

    def initialize(@value : T, @read_bytes : Int32 = 0)
    end

    def self.decode(io : IO)
      value = T.new(0)
      i = 0
      read_bytes = 0

      read = ->{
        read_bytes += 1
        io.read_bytes(Int8, IO::ByteFormat::BigEndian)
      }

      while ((b = read.call) & 0x80) != 0
        value |= (b & 0x7f) << i
        i += 7
        raise "illegalZigZagException(#{value})" if (i > {{ T.stringify.gsub(/^.*?(\d+).*$/, "\\1").id }} - 1)
      end
      value |= b << i
      v = (value >> 1) ^ -(value & 1)

      return new(v, read_bytes: read_bytes)
    end
  end

  alias Varint  = Var(Int32)
  alias Varlong = Var(Int64)
end
