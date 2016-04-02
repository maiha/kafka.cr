require "./spec_helper"

describe Kafka::Commands::Fetch do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 't1' --replication-factor=1 --partitions 1
  # echo "test" | ./kafka-console-producer.sh --topic "t1" --broker-list "localhost:9092"

  describe "#fetch" do
    # TODO: this test expects "t1" topic exists
    it "returns Kafka::Message when topic and message exists (t1,0,0)" do
      #      kafka.handler.request = ->(req: Kafka::Request) { p req }
      #      kafka.handler.respond = ->(res: Kafka::Response) { p res }
      mes = kafka.fetch("t1", 0, 0_i64)
      expect(mes).to be_a(Kafka::Message)
      expect(mes.index).to eq(Kafka::Index.new("t1", 0, 0_i64))
    end

    it "raises not found exception when missing (_t1,0,0)" do
      expect{ kafka.fetch("_t1", 0, 0_i64) }.to raise_error(Kafka::MessageNotFound)
    end
  end
end
