require "./spec_helper"

describe Kafka::Protocol::FetchRequest do
  it "to_kafka" do
    partition = Structure::FetchRequestPartitions.new(
      partition = 0,
      offset = 0_i64,
      max_bytes = 1024
    )
    tfos = [Structure::FetchRequestTopics.new("t1", [partition])]

    req = Kafka::Protocol::FetchRequest.new(
      correlation_id = 0,
      client_id = "kafka-fetch",
      replica = -1,
      max_wait_time = 100,
      min_bytes = 64*1000,
      topics = tfos
    )
    bin = req.to_slice
    bin.should eq(bytes(0, 0, 0, 61, 0, 1, 0, 0, 0, 0, 0, 0, 0, 11, 107, 97, 102, 107, 97, 45, 102, 101, 116, 99, 104, 255, 255, 255, 255, 0, 0, 0, 100, 0, 0, 250, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0))
  end
end

describe Kafka::Protocol::FetchResponse do
  describe "(3: UnknownTopicOrPartitionCode)" do
    it "from_kafka" do
      res = Kafka::Protocol::FetchResponse.from_kafka(bytes(0, 0, 0, 45, 0, 0, 0, 0, 0, 0, 0, 1, 0, 13, 115, 101, 118, 101, 110, 46, 99, 105, 46, 104, 116, 116, 112, 0, 0, 0, 1, 0, 0, 0, 0, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0))
      res.correlation_id.should eq(0)
      res.topics.size.should eq(1)

      topic = res.topics.first.not_nil!
      topic.partitions.size.should eq(1)

      partition = topic.partitions.first.not_nil!
      partition.partition.should eq(0)
      partition.error_code.should eq(3)
      partition.high_water_mark.should eq(-1)
      #      partition.message_sets.size.should eq(0)
    end
  end

  describe "(1 data)" do
    it "from_kafka" do
      res = Kafka::Protocol::FetchResponse.from_kafka(bytes(0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))

      #      0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116
      res.correlation_id.should eq(0)
      res.topics.size.should eq(1)

      topic = res.topics.first.not_nil!
      topic.partitions.size.should eq(1)

      partition = topic.partitions.first.not_nil!
      partition.partition.should eq(0)
      partition.error_code.should eq(0)
      partition.high_water_mark.should eq(1)
      #      partition.message_sets.size.should eq(0)
    end
  end
end
