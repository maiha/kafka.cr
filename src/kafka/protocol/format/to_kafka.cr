module Kafka::Protocol::Format
  def to_kafka(io : IO, value)
    io.write_bytes(value, IO::ByteFormat::BigEndian)
  end

  def to_kafka(io : IO, value : Int16)
    io.write_bytes(value.to_u16, IO::ByteFormat::BigEndian)
  end

  def to_kafka(io : IO, value : Int32)
    io.write_bytes(value.to_u32, IO::ByteFormat::BigEndian)
  end

  def to_kafka(io : IO, value : Kafka::Protocol::Structure)
    value.to_kafka(io)
  end

  def to_kafka(io : IO, value : String)
    io.write_bytes(value.bytesize.to_u16, IO::ByteFormat::BigEndian)
    io.write(value.to_slice)
  end

  def to_kafka(io : IO, array : Array(Object))
    io.write_bytes(array.size.to_u32, IO::ByteFormat::BigEndian)
    array.each do |obj|
      to_kafka(io, obj)
    end
  end

  def to_kafka(io : IO, bytes : Slice)
    bytes.each { |byte| io.write_byte byte.to_u8 }
  end

  def to_kafka(io : IO, value : Int32)
    io.write_bytes(value.to_u32, IO::ByteFormat::BigEndian)
  end
end
