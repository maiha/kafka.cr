require "./spec_helper"

include Kafka::Protocol

describe HeartbeatRequest do
  it "create request binary" do
    req = HeartbeatRequest.new
    bin = req.to_bytes
    bin.should eq(bytes(0, 12, 0, 0, 0, 0, 0, 1, 0, 2, 99, 114, 0, 1, 120, 255, 255, 255, 255, 0, 2, 99, 114))
  end
end

describe HeartbeatResponse do
  it "instantiate from binary" do
    io = MemoryIO.new(bytes(0, 0, 0, 6, 0, 0, 0, 1, 0, 16))
    res = HeartbeatResponse.from_io(io)
    res.correlation_id.should eq(1)
    res.error_code.should eq(16)
  end
end

