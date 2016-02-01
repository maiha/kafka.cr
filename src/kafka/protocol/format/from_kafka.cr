def Int32.from_kafka(io : IO)
  io.read_bytes(Int32, IO::ByteFormat::BigEndian)
end

def Int16.from_kafka(io : IO)
  io.read_bytes(Int16, IO::ByteFormat::BigEndian)
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
