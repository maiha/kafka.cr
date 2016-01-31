require "./spec_helper"

describe Kafka::Protocol::Structure::MetadataRequest do
  it "to_io" do
    io = MemoryIO.new
    req = Kafka::Protocol::Structure::MetadataRequest.new(3_i16, 0_i16, 0, "x", ["t1"])
    req.to_io(io)
    io.to_slice.should eq(bytes(0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 0, 0, 1, 0, 2, 116, 49))
  end
end

describe Kafka::Protocol::MetadataRequest do
  it "to_io" do
    req = Kafka::Protocol::MetadataRequest.new(0, "x", ["t1"])
    bin = req.to_slice
    bin.should eq(bytes(0, 0, 0, 19, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 0, 0, 1, 0, 2, 116, 49))
  end
end

### response
# (no topics)
# [0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 0]
#
# (t1)
# [0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 6, 117, 98, 117, 110, 116, 117, 0, 0, 35, 132, 0, 0, 0, 1, 0, 0, 0, 2, 116, 49, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1]
