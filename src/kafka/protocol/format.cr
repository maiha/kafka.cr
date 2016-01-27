module Kafka::Protocol::Format
  def write(io : IO, value)
    io.write_bytes(value, IO::ByteFormat::BigEndian)
  end

  def write(io : IO, value : String)
    io.write_bytes(value.bytesize.to_u16, IO::ByteFormat::BigEndian)
    io.write(value.to_slice)
  end

  def write(io : IO, array : Array(Object))
    io.write_bytes(array.size.to_u32, IO::ByteFormat::BigEndian)
    array.each do |obj|
      write(io, obj)
    end
  end

  def write(io : IO, bytes : Slice)
    bytes.each { |byte| io.write_byte byte.to_u8 }
  end

  def write(io : IO, value : Int32)
    io.write_bytes(value.to_u32, IO::ByteFormat::BigEndian)
  end
end
