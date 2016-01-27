module Kafka::Protocol
  # read raw binary data into slice
  def self.read(io : IO) : Slice
    size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
    body = Slice(UInt8).new(size)
    io.read_fully(body)

    out = MemoryIO.new(4 + size)
    out.write_bytes(size, format = IO::ByteFormat::BigEndian)
    out.write(body)
    out.to_slice
  end
end
