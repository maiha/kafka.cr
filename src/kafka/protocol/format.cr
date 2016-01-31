module Kafka::Protocol::Format
  module FromIO
    def from_io(io : IO, type)
      io.read_bytes(type, IO::ByteFormat::BigEndian)  # drop checksum
    end

    extend self
  end

  def to_io(io : IO, value)
    io.write_bytes(value, IO::ByteFormat::BigEndian)
  end

  def to_io(io : IO, value : Int16)
    io.write_bytes(value.to_u16, IO::ByteFormat::BigEndian)
  end

  def to_io(io : IO, value : Int32)
    io.write_bytes(value.to_u32, IO::ByteFormat::BigEndian)
  end

  def to_io(io : IO, value : Kafka::Protocol::Structure)
    value.to_io(io)
  end

  def to_io(io : IO, value : String)
    io.write_bytes(value.bytesize.to_u16, IO::ByteFormat::BigEndian)
    io.write(value.to_slice)
  end

  def to_io(io : IO, array : Array(Object))
    io.write_bytes(array.size.to_u32, IO::ByteFormat::BigEndian)
    array.each do |obj|
      to_io(io, obj)
    end
  end

  def to_io(io : IO, bytes : Slice)
    bytes.each { |byte| io.write_byte byte.to_u8 }
  end

  def to_io(io : IO, value : Int32)
    io.write_bytes(value.to_u32, IO::ByteFormat::BigEndian)
  end

end
