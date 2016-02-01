module Kafka::Protocol::Request::Format
  # header and bytes
  def to_kafka(io : IO)
    body = to_bytes
    head = body.size.to_u32
    write(io, head)
    write(io, body)
  end

  # header and bytes
  def to_binary : Slice
    io = MemoryIO.new
    to_kafka(io)
    io.to_slice
  end

  # only bytes
  def to_bytes : Slice
    io = MemoryIO.new
    to_bytes(io)
    io.to_slice
  end
end
