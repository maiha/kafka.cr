require "./spec_helper"

describe Kafka::Commands::Offset do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  describe "#offset" do
    it "returns Kafka::Message when topic exists (test,0)" do
      offset = kafka.offset("test", 0)
      expect(offset).to be_a(Kafka::Offset)
      expect(offset.index.topic).to eq("test")
      expect(offset.index.partition).to eq(0)
      expect(offset.count).to_be >= 1
    end

    it "raises not found exception when topic is missing (_tmp,0)" do
      expect{ kafka.offset("_tmp", 0) }.to raise_error(Kafka::OffsetNotFound)
    end
  end
end
