require "./spec_helper"

describe Kafka::Protocol::ProduceV0Request do
  include Kafka::Protocol

  def partition_message(partition : Int32, bytes : Array(Slice(UInt8)))
    ms = bytes.map{|b| Structure::MessageSet.new(offset = 0.to_i64, Structure::Message.new(b))}
    Structure::PartitionMessage.new(partition, Structure::MessageSetEntry.new(ms))
  end

  def partition_message(partition : Int32, bytes : Slice(UInt8))
    partition_message(partition, [bytes])
  end

  def partition_message
    [] of Structure::PartitionMessage
  end

  describe "(empty messages)" do
    it "to_kafka" do
      data = [] of Structure::TopicAndPartitionMessages
      req = ProduceV0Request.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      expect(req.to_slice).to eq(bytes(0, 0, 0, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 0))
    end
  end

  describe "(1 message)" do
    it "to_kafka" do
      pm = partition_message(0, bytes(1,2))
      data = [Structure::TopicAndPartitionMessages.new("t", [pm])]
      req = ProduceV0Request.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      expect(req.to_slice).to eq(bytes(0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 1, 0, 1, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 147, 49, 213, 229, 0, 0, 255, 255, 255, 255, 0, 0, 0, 2, 1, 2))
    end
  end

  describe "(2 messages)" do
    let(pm1) { partition_message(0, bytes(1,2)) }
    let(pm2) { partition_message(0, bytes(3,4,5)) }
    let(data) { [Structure::TopicAndPartitionMessages.new("t", [pm1, pm2])] }

    it "to_kafka" do
      req = ProduceV0Request.new(0, "x", 1.to_i16, 1000.to_i32, data)
                                              
      expect(req.to_slice).to eq(bytes(0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 120, 0, 1, 0, 0, 3, 232, 0, 0, 0, 1, 0, 1, 116, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 147, 49, 213, 229, 0, 0, 255, 255, 255, 255, 0, 0, 0, 2, 1, 2, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 159, 249, 40, 173, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 3, 4, 5))
    end
  end
end
