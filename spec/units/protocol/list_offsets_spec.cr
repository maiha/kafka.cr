require "./spec_helper"

describe Kafka::Protocol::ListOffsetsRequest do
  include Kafka::Protocol

  let(partition) { Structure::Partition.new(p = 0, latest_offset = -1_i64, max_offsets = 999999999) }
  let(taps) { [Structure::TopicAndPartitions.new("t1", [partition])] }

  let(req) {
    Kafka::Protocol::ListOffsetsRequest.new(
      correlation_id = 0,
      client_id = "kafka-offset",
      replica = -1,
      taps
    )
  }

  it "to_kafka" do
    expect(req.to_slice).to eq(bytes(0, 2, 0, 0, 0, 0, 0, 0, 0, 12, 107, 97, 102, 107, 97, 45, 111, 102, 102, 115, 101, 116, 255, 255, 255, 255, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 59, 154, 201, 255))
  end
end

describe Kafka::Protocol::ListOffsetsResponse do
  describe "(1 topic)" do
    let(res) {
      Kafka::Protocol::ListOffsetsResponse.from_kafka(bytes(0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0))
    }

    let(tpo) { res.topic_partition_offsets.first.not_nil! }
    let(po) { tpo.partition_offsets.first.not_nil! }

    it "from_kafka" do
      expect(res.correlation_id).to eq(0)
      expect(res.topic_partition_offsets.size).to eq(1)

      expect(tpo.topic).to eq("t1")
      expect(tpo.partition_offsets.size).to eq(1)

      expect(po.partition).to eq(0)
      expect(po.error_code).to eq(0)
      expect(po.offsets).to eq([5, 0])
    end
  end
end
