require "./spec_helper"

describe Kafka::Protocol::Structure::Message do
  include Kafka::Protocol::Structure

  describe "codec" do
    let(m1) { Message.new(
      crc = 1,
      magic_byte = 2.to_i8,
      attributes = 3.to_i8,
      key = bytes(4),
      value = bytes(5,6)
    )}

    it "creates binary and restore from it" do
      io = MemoryIO.new
      m1.to_kafka(io)

      io.rewind
      m2 = Message.from_kafka(io)
      expect(m2.crc).to eq(1)
      expect(m2.magic_byte).to eq(2)
      expect(m2.attributes).to eq(3)
      expect(m2.key).to eq(bytes(4))
      expect(m2.value).to eq(bytes(5,6))
    end
  end

  it "calculates crc" do
    key = Slice(UInt8).new(0)
    val = bytes(50, 48, 48)
    mes = Message.new(key, val)

    expect(mes.magic_byte).to eq(0)
    expect(mes.attributes).to eq(0)
    expect(mes.crc).to eq(1472827614)
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
    expect(io.to_slice).to eq(bytes(89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
  end
end
