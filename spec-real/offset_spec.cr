require "./spec_helper"

describe Kafka::Commands::Offset do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 't1' --replication-factor=1 --partitions 1
  # echo "test" | ./kafka-console-producer.sh --topic "t1" --broker-list "localhost:9092"

  describe "#offset" do
    # TODO: this test expects "t1" topic exists
    it "returns Kafka::Message when topic exists (t1,0)" do
      offset = kafka.offset("t1", 0)
      expect(offset).to be_a(Kafka::Offset)
      expect(offset.index.topic).to eq("t1")
      expect(offset.index.partition).to eq(0)
      expect(offset.count).to_be >= 1
    end

    it "raises not found exception when topic is missing (_t1,0)" do
      expect{ kafka.offset("_t1", 0) }.to raise_error(Kafka::OffsetNotFound)
    end
  end
end
