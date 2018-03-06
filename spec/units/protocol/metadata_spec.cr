require "./spec_helper"

include Kafka::Protocol

describe Kafka::Protocol::MetadataRequest do
  it "to_kafka" do
    req = Kafka::Protocol::MetadataRequest.new(0, "x", ["t1"])
    expect(req.to_slice).to eq(bytes(0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 0, 0, 1, 0, 2, 116, 49))
  end
end

describe Kafka::Protocol::MetadataResponse do
  describe "(no topics)" do
    it "from_kafka" do
      res = Kafka::Protocol::MetadataResponse.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 0))
      expect(res.correlation_id).to eq(0)
      expect(res.brokers).to eq([Kafka::Protocol::Structure::Broker.new(1, "ubuntu", 9092)])
      expect(res.topics).to eq([] of Kafka::Protocol::Structure::TopicMetadata)
    end
  end

  describe "(1 topic)" do
    let(topic) { res.topics.first.not_nil! }
    let(partition) { topic.partitions.first.not_nil! }
    let(broker) { res.brokers.first.not_nil! }
    
    let(res) {
      Kafka::Protocol::MetadataResponse.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 1, 0, 0, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1))
    }
    
    it "from_kafka" do
      expect(res.correlation_id).to eq(0)
      expect(res.brokers.size).to eq(1)

      expect(broker).to be_a(Structure::Broker)
      expect(broker.node_id).to eq(1)
      expect(broker.host).to eq("ubuntu")
      expect(broker.port).to eq(9092)

      expect(topic).to be_a(Structure::TopicMetadata)
      expect(topic.error_code).to eq(0)
      expect(topic.name).to eq("t1")
      expect(topic.partitions.size).to eq(1)

      expect(partition).to be_a(Structure::PartitionMetadata)
      expect(partition.error_code).to eq(0)
      expect(partition.id).to eq(0)
      expect(partition.leader).to eq(1)
      expect(partition.replicas).to eq([1])
      expect(partition.isrs).to eq([1])
    end
  end
end
