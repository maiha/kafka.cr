require "./spec_helper"

describe Kafka::Protocol::FetchRequest do
  include Kafka::Protocol

  describe "to_kafka" do
    let(partition) { Structure::FetchRequestPartitions.new(
      partition = 0,
      offset = 0_i64,
      max_bytes = 1024
    )}

    let(tfos) { [Structure::FetchRequestTopics.new("t1", [partition])] }
    let(req) {
      Kafka::Protocol::FetchRequest.new(
        correlation_id = 0,
        client_id = "kafka-fetch",
        replica = -1,
        max_wait_time = 100,
        min_bytes = 64*1000,
        topics = tfos
      )
    }

    it "creates binary" do
      expect(req.to_slice).to eq(bytes(0, 1, 0, 0, 0, 0, 0, 0, 0, 11, 107, 97, 102, 107, 97, 45, 102, 101, 116, 99, 104, 255, 255, 255, 255, 0, 0, 0, 100, 0, 0, 250, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0))
    end
  end
end

describe Kafka::Protocol::FetchResponse do
  let(topic) { res.topics.first.not_nil! }
  let(partition) { topic.partitions.first.not_nil! }

  describe "(3: UnknownTopicOrPartitionCode)" do
    let(res) {
      Kafka::Protocol::FetchResponse.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 1, 0, 13, 115, 101, 118, 101, 110, 46, 99, 105, 46, 104, 116, 116, 112, 0, 0, 0, 1, 0, 0, 0, 0, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0))
    }

    it "creates object" do
      expect(res.correlation_id).to eq(0)
      expect(res.topics.size).to eq(1)

      expect(topic.partitions.size).to eq(1)

      expect(partition.partition).to eq(0)
      expect(partition.error_code).to eq(3)
      expect(partition.high_water_mark).to eq(-1)
    end
  end

  describe "(1 data)" do
    let(res) {
      Kafka::Protocol::FetchResponse.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 18, 89, 42, 71, 135, 0, 0, 255, 255, 255, 255, 0, 0, 0, 4, 116, 101, 115, 116))
    }

    it "creates object" do
      expect(res.correlation_id).to eq(0)
      expect(res.topics.size).to eq(1)

      expect(topic.partitions.size).to eq(1)

      expect(partition.partition).to eq(0)
      expect(partition.error_code).to eq(0)
      expect(partition.high_water_mark).to eq(1)
    end
  end
end
