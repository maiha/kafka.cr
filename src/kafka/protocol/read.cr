module Kafka::Protocol
  # read raw kafka binary data as slice used by verbose mode in applications
  def self.read(io : IO) : Slice
    body = Kafka::Protocol.from_kafka(io)
    size = body.size

    out = IO::Memory.new(4 + size)
    out.write_bytes(size, format = IO::ByteFormat::BigEndian)
    out.write(body.to_slice)
    out.to_slice
  end
end
