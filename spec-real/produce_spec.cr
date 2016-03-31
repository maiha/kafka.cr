require "./spec_helper"

describe Kafka::Commands::Produce do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 'tmp' --replication-factor=1 --partitions 1

  describe "#produce_v1" do
    # TODO: this test expects "tmp" topic exists
    it "returns Kafka::Message when exists" do
      mes = kafka.produce_v1("tmp", 0, "test")
      expect(mes).to be_a(Kafka::Protocol::ProduceV1Response)
    end

    it "raises not found exception when missing" do
#      expect{
#        mes = kafka.produce_v1("xxx", 0, "test")
#      }.to raise_error(Kafka::MessageNotFound)
    end
  end
end
