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

  describe "from_kafka" do
    let(io) { IO::Memory.new(bytes(0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116)) }
    let(entry) { MessageSetEntry.from_kafka(io) }
    let(set) { entry.message_sets[0].not_nil! }

    it "creates object" do
      expect(entry.size).to eq(30)

      expect(set.offset).to eq(0)
      expect(set.bytesize).to eq(18)
      expect(set.message.crc).to eq(1495943047)
      expect(set.message.magic_byte).to eq(0)
      expect(set.message.attributes).to eq(0)
      expect(set.message.key).to eq(Null)
      expect(set.message.value).to eq("test".to_slice)
    end
  end

  describe "to_kafka" do
    let(entry) { MessageSetEntry.new([MessageSet.new("test")]) }
    let(io) { IO::Memory.new }

    it "create binary" do
      entry.to_kafka(io)
      expect(io.to_slice).to eq(bytes(0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
    end
  end
end
