require "./spec_helper"

describe Kafka::Protocol::OffsetRequest do
  it "to_kafka" do
    partition = Structure::Partition.new(p = 0, latest_offset = -1_i64, max_offsets = 999999999)
    taps = [Structure::TopicAndPartitions.new("t1", [partition])]

    req = Kafka::Protocol::OffsetRequest.new(
      correlation_id = 0,
      client_id = "kafka-offset",
      replica = -1,
      taps
    )
    bin = req.to_slice
    bin.should eq(bytes(0, 0, 0, 54, 0, 2, 0, 0, 0, 0, 0, 0, 0, 12, 107, 97, 102, 107, 97, 45, 111, 102, 102, 115, 101, 116, 255, 255, 255, 255, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255))
  end
end

describe Kafka::Protocol::OffsetResponse do
  describe "(1 topic)" do
    it "from_kafka" do
      res = Kafka::Protocol::OffsetResponse.from_kafka(bytes(0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0))
      res.correlation_id.should eq(0)
      res.topic_partition_offsets.size.should eq(1)

      tpo = res.topic_partition_offsets.first.not_nil!
      tpo.topic.should eq("t1")
      tpo.partition_offsets.size.should eq(1)

      po = tpo.partition_offsets.first.not_nil!
      po.partition.should eq(0)
      po.error_code.should eq(0)
      po.offsets.should eq([5, 0])
    end
  end
end
