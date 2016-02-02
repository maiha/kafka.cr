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
