# #####################################################################
# ## from kafka

def Int8.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  io_read_bytes_with_debug(:cyan)
end

def Int16.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  io_read_bytes_with_debug(:cyan)
end

def Int32.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  io_read_bytes_with_debug(:cyan)
end

def Int64.from_kafka(io : IO, debug_level = -1, hint = "")
  on_debug_head_address
  io_read_bytes_with_debug(:cyan)
end

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
  on_debug "#{label}".colorize(:cyan)
  ary = new
  len = io_read_int32
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

struct Int8
  def to_kafka(io : IO)
    io.write_bytes(to_u8, IO::ByteFormat::BigEndian)
  end
end

struct Int16
  def to_kafka(io : IO)
    io.write_bytes(to_u16, IO::ByteFormat::BigEndian)
  end
end

struct Int32
  def to_kafka(io : IO)
    io.write_bytes(to_u32, IO::ByteFormat::BigEndian)
  end
end

struct Int64
  def to_kafka(io : IO)
    io.write_bytes(to_u64, IO::ByteFormat::BigEndian)
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
