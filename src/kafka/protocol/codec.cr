######################################################################
### from kafak

def Int16.from_kafka(io : IO)
  io.read_bytes(Int16, IO::ByteFormat::BigEndian)
end

def Int32.from_kafka(io : IO)
  io.read_bytes(Int32, IO::ByteFormat::BigEndian)
end

def Int64.from_kafka(io : IO)
  io.read_bytes(Int64, IO::ByteFormat::BigEndian)
end

def String.from_kafka(io : IO)
  len = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
  slice = Slice(UInt8).new(len).tap { |s| io.read_fully(s) }
  String.new(slice)
end

def Array.from_kafka(io : IO)
  ary = new
  len = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
  (1..len).each do
    ary << T.from_kafka(io)
  end
  return ary
end

######################################################################
### to kafak
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
    io.write_bytes(bytesize.to_u16, IO::ByteFormat::BigEndian)
    io.write(to_slice)
  end
end

class Array
  def to_kafka(io : IO)
    io.write_bytes(size.to_u32, IO::ByteFormat::BigEndian)
    each(&.to_kafka(io))
  end
end

