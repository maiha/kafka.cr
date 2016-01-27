require "./spec_helper"

include Kafka::Protocol

describe HeartbeatRequest do
  it "create request" do
    req = HeartbeatRequest.new
    bin = req.to_bytes
    bin.should eq(bytes(0, 12, 0, 0, 0, 0, 0, 1, 0, 2, 99, 114, 0, 1, 120, 255, 255, 255, 255, 0, 2, 99, 114))
  end
end
