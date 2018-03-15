require "./spec_helper"

describe Kafka::Protocol::Structure::Builder do
  include Kafka::Protocol::Structure

  describe "(a: 3 partitions, b: 1 partition)" do
    it "build" do
      res = Kafka::Protocol::MetadataResponseV0.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 0, 0, 35, 132, 0, 0, 0, 2, 0, 9, 108, 111, 99, 97, 108, 104, 111, 115, 116, 0, 0, 35, 133, 0, 0, 0, 2, 0, 0, 0, 1, 98, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 97, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0, 1))

      # a#0     leader=2,replica=[2, 1],isr=[2, 1]
      # a#1     leader=1,replica=[1, 2],isr=[1, 2]
      # a#2     leader=2,replica=[2, 1],isr=[2, 1]
      # b#0     leader=2,replica=[2],isr=[2]

      req = res.to_offset_requests
      expect(req.keys.sort).to eq([1, 2])

      # [leader:1]
      # => [("a", [1])]
      expect(req[1].not_nil!.pretty_topic_partitions).to eq({"a" => [1]})

      # [leader:2]
      # => [("a", [0,2]), ("b", [0])]
      expect(req[2].not_nil!.pretty_topic_partitions).to eq({"b" => [0], "a" => [2, 0]})
    end
  end
end
