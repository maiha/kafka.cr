require "./spec_helper"

include Kafka::Protocol::Structure

describe Kafka::Protocol::Structure::Message do
  it "codec" do
    m1 = Message.new(
      crc = 1,
      magic_byte = 2.to_i8,
      attributes = 3.to_i8,
      key = bytes(4),
      value = bytes(5,6)
    )

    io = MemoryIO.new
    m1.to_kafka(io)

    io.rewind
    m2 = Message.from_kafka(io)
    m2.crc.should eq(1)
    m2.magic_byte.should eq(2)
    m2.attributes.should eq(3)
    m2.key.should eq(bytes(4))
    m2.value.should eq(bytes(5,6))
  end
end
