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
