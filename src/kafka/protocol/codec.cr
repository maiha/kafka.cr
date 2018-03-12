# #####################################################################
# ## from kafka

def Kafka::NullableString.from_kafka(io : IO, debug_level = -1, hint = "")
  String.from_kafka(io, debug_level, hint)
end

{% for klass in %w( Bool Int8 Int16 Int32 UInt32 Int64 UInt64 ) %}
  def {{klass.id}}.from_kafka(io : IO, debug_level = -1, hint = "")
    on_debug_head_address
    io_read_bytes_with_debug(:cyan, {{klass.id}})
  end
{% end %}

def String.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  name = hint.to_s.empty? ? "" : "(#{hint})"

  len = io_read_int16

  if len == -1
    on_debug "String[2]#{name} -> (null)".colorize(:cyan)
    return ""
  else
    slice = Slice(UInt8).new(len).tap { |s| io.read_fully(s) }
    str = String.new(slice)
    on_debug "String[2]#{name} -> (#{len})#{str.inspect}".colorize(:cyan)
    return str
  end
end

def Slice.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  len = io_read_int32

  name = hint.to_s.empty? ? "" : "(#{hint})"
  if len == -1
    on_debug "Binary[4]#{name} -> (-1)(null)".colorize(:cyan)
    Slice(UInt8).new(0)
  elsif len == 0
    on_debug "Binary[4]#{name} -> (0)(zero?)".colorize(:red)
    Slice(UInt8).new(0)
  else
    binary = Slice(UInt8).new(len).tap { |s| io.read_fully(s) }
    on_debug "Binary[4]#{name} -> (#{len})#{binary.inspect}".colorize(:cyan)
    return binary
  end
end

def Array.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  label = self.to_s.sub(/Kafka::Protocol::Structure::/, "").sub(/^Array/, "Array[4]")
  ary = new
  len = io_read_int32
  on_debug "#{label} -> #{len}".colorize(:cyan)
  (1..len).each do
    ary << T.from_kafka(io, debug_level_succ)
  end
  return ary
end

######################################################################
### to kafka

struct Nil
  def to_kafka(io : IO)
    Kafka::Protocol::Structure::Null.to_kafka(io)
  end
end

struct Kafka::NullableString
  def to_kafka(io : IO)
    "".to_kafka(io)
  end
end

{% for klass in %w( Int8 Int16 Int32 UInt32 Int64 UInt64 ) %}
  {% size = klass.gsub(/[a-z]/i, "") %}
  struct {{klass.id}}
    def to_kafka(io : IO)
      io.write_bytes(to_u{{size.id}}, IO::ByteFormat::BigEndian)
    end
  end
{% end %}

struct Bool
  def to_kafka(io : IO)
    io.write_bytes(self ? 1_u8 : 0_u8, IO::ByteFormat::BigEndian)
  end
end

class String
  def to_kafka(io : IO)
    if bytesize == 0
      # A length of -1 indicates null
      io.write_bytes(-1.to_u16, IO::ByteFormat::BigEndian)
    else
      io.write_bytes(bytesize.to_u16, IO::ByteFormat::BigEndian)
      io.write(to_slice)
    end
  end
end

struct Slice(T)
  def to_kafka(io : IO)
    if bytesize == 0
      # A length of -1 indicates null
      io.write_bytes(-1.to_u32, IO::ByteFormat::BigEndian)
    else
      io.write_bytes(bytesize.to_u32, IO::ByteFormat::BigEndian)
      io.write(to_slice)
    end
  end
end

class Array
  def to_kafka(io : IO)
    io.write_bytes(size.to_u32, IO::ByteFormat::BigEndian)
    each(&.to_kafka(io))
  end
end

module Kafka::Protocol
  # https://kafka.apache.org/protocol
  def self.from_kafka(io : IO) : IO
    # RequestOrResponse => Size (RequestMessage | ResponseMessage)
    # Size => int32
    size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    body = Slice(UInt8).new(size)

    # first read full data to avoid runtime "Illegal seek"
    io.read_fully(body)

    return IO::Memory.new(body)
  end
end
