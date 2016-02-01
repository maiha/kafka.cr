require "./spec_helper"

describe Kafka::Protocol::OffsetRequest do
  it "to_kafka" do
    req = Kafka::Protocol::OffsetRequest.new(
          correlation_id = 0,
          client_id = "x",
          replica = 1,
          topics = [] of Kafka::Protocol::Structure::TopicAndPartitions
        )
    bin = req.to_slice
    bin.should eq(bytes(0, 0, 0, 19, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 0, 0, 1, 0, 0, 0, 0))
  end
end

