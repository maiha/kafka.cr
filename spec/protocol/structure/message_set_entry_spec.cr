require "./spec_helper"

include Kafka::Protocol::Structure

describe Kafka::Protocol::Structure::MessageSetEntry do
  # MessageSetEntry
  # (0000030)              Int32[4](size) -> 30
  # (MessageSet.from_kafka)
  # (0000034)                Int64[8](offset) -> 0
  # (0000042)                Int32[4](bytesize) -> 18
  # (Message.from_kafka)
  # (0000046)                  Int32[4](crc) -> 1495943047
  # (0000050)                  Int8[1](magic_byte) -> 0
  # (0000051)                  Int8[1](attributes) -> 0
  # (0000052)                  Binary[4](key) -> (-1)(null)
  # (0000056)                  Binary[4](value) -> (4)[116, 101, 115, 116]

  it "from_kafka" do
    io = MemoryIO.new(bytes(0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
    entry = MessageSetEntry.from_kafka(io)

    entry.size.should eq(30)

    set = entry.message_sets[0].not_nil!
    set.offset.should eq(0)
    set.bytesize.should eq(18)
    set.message.crc.should eq(1495943047)
    set.message.magic_byte.should eq(0)
    set.message.attributes.should eq(0)
    set.message.key.should eq(Null)
    set.message.value.should eq("test".to_slice)
  end

  it "to_kafka" do
    entry = MessageSetEntry.new([MessageSet.new("test")])

    io = MemoryIO.new
    entry.to_kafka(io)
    io.to_slice.should eq(bytes(0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
  end
end
