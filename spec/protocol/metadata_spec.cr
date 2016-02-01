require "./spec_helper"

include Kafka::Protocol

describe Kafka::Protocol::MetadataRequest do
  it "to_kafka" do
    req = Kafka::Protocol::MetadataRequest.new(0, "x", ["t1"])
    bin = req.to_slice
    bin.should eq(bytes(0, 0, 0, 19, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 0, 0, 1, 0, 2, 116, 49))
  end
end

describe Kafka::Protocol::MetadataResponse do
  describe "(no topics)" do
    it "from_kafka" do
      res = Kafka::Protocol::MetadataResponse.from_kafka(bytes(0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 0))
      res.correlation_id.should eq(0)
      res.brokers.should eq([Kafka::Protocol::Structure::Broker.new(1, "ubuntu", 9092)])
      res.topics.should eq([] of Kafka::Protocol::Structure::TopicMetadata)
    end
  end

  describe "(1 topic)" do
    it "from_kafka" do
      res = Kafka::Protocol::MetadataResponse.from_kafka(bytes(0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 1, 0, 0, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1))
      res.correlation_id.should eq(0)
      res.brokers.size.should eq(1)

      broker = res.brokers.first.not_nil!
      broker.should be_a(Structure::Broker)
      broker.node_id.should eq(1)
      broker.host.should eq("ubuntu")
      broker.port.should eq(9092)

      topic = res.topics.first.not_nil!
      topic.should be_a(Structure::TopicMetadata)
      topic.error_code.should eq(0)
      topic.name.should eq("t1")
      topic.partitions.size.should eq(1)

      partition = topic.partitions.first.not_nil!
      partition.should be_a(Structure::PartitionMetadata)
      partition.error_code.should eq(0)
      partition.id.should eq(0)
      partition.leader.should eq(1)
      partition.replicas.should eq([1])
      partition.isrs.should eq([1])
    end
  end
end
