require "./spec_helper"

include Kafka::Protocol

describe MetadataRequest do
  it "create request" do
    req = MetadataRequest.new
    bin = req.to_slice
    bin.should eq(u8(0, 3, 0, 0, 0, 0, 0, 1, 0, 2, 99, 114))
  end
end
