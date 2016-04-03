require "./spec_helper"

describe Kafka::Commands::Fetch do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 't1' --replication-factor=1 --partitions 1
  # echo "test" | ./kafka-console-producer.sh --topic "t1" --broker-list "localhost:9092"

  # TODO: this test expects "tmp" topic exists
  describe "#fetch" do
    it "raises protocol error when missing (_tmp,0,0)" do
      expect {
        kafka.fetch("_tmp")
      }.to raise_error(Kafka::Protocol::Error)
    end

    it "returns Kafka::Message" do
      body = Time.now.to_s
      res = kafka.produce("tmp", body)
      mes = kafka.fetch("tmp")
      expect(mes).to be_a(Kafka::Message)
#      expect(mes.value.string).to eq(body)
    end
  end
end
