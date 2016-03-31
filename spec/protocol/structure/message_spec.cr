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

  it "calculates crc" do
    key = Slice(UInt8).new(0)
    val = bytes(50, 48, 48)
    mes = Message.new(key, val)

    mes.magic_byte.should eq(0)
    mes.attributes.should eq(0)
    mes.crc.should eq(1472827614)
  end

  it "to_kafka with null key" do
    # (Message.from_kafka)
    # (0000046)                  Int32[4](crc) -> 1495943047
    # (0000050)                  Int8[1](magic_byte) -> 0
    # (0000051)                  Int8[1](attributes) -> 0
    # (0000052)                  Binary[4](key) -> (-1)(null)
    # (0000056)                  Binary[4](value) -> (4)[116, 101, 115, 116]

    mes = Message.new(0_i8, 0_i8, Null, "test".to_slice)

    io = MemoryIO.new
    mes.to_kafka(io)
    io.to_slice.should eq(bytes(89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
  end
end
