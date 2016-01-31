require "./spec_helper"

include Kafka::Protocol

describe MetadataRequest do
  it "create request binary" do
    req = MetadataRequest.new
    req.topics = [] of String
    bin = req.to_bytes
    bin.should eq(bytes(0, 3, 0, 0, 0, 0, 0, 1, 0, 2, 99, 114, 0, 0, 0, 0))
  end
end

describe MetadataResponse do
  it "instantiate from binary" do
    io = MemoryIO.new(bytes(0, 0, 0, 28, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 0))
    res = MetadataResponse.from_io(io)
    res.correlation_id.should eq(1)
    res.error_code.should eq(0)
  end
end
