require "./spec_helper"

include Kafka::Protocol

describe Kafka::Protocol::ProduceRequest do
  describe "(empty messages)" do
    it "to_kafka" do
      data = [] of Structure::TopicAndPartitionMessages
      req = Kafka::Protocol::ProduceRequest.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      bin = req.to_slice
      bin.should eq(bytes(0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 0))
    end
  end

  describe "(1 message)" do
    it "to_kafka" do
      pm = partition_message(0, bytes(1,2))
      data = [Structure::TopicAndPartitionMessages.new("t", [pm])]
      req = Kafka::Protocol::ProduceRequest.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      bin = req.to_slice
      bin.should eq(bytes(0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 1, 0, 1, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 123, 213, 198, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2))
    end
  end

  describe "(2 messages)" do
    it "to_kafka" do
      pm1 = partition_message(0, bytes(1,2))
      pm2 = partition_message(0, bytes(3,4,5))
      data = [Structure::TopicAndPartitionMessages.new("t", [pm1, pm2])]
      req = Kafka::Protocol::ProduceRequest.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      bin = req.to_slice
      bin.should eq(bytes(0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 1, 0, 1, 116, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 123, 213, 198, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 15, 116, 70, 130, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 4, 5))
    end
  end
end
