require "./spec_helper"

describe Kafka::Commands::Produce do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 'tmp' --replication-factor=1 --partitions 1

  describe "#produce_v0" do
    # TODO: this test expects "tmp" topic exists
    it "returns Kafka::Message when exists" do
      mes = kafka.produce_v0("tmp", 0, "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceV0Response)
    end

    it "raises not found exception when missing" do
#      expect{
#        mes = kafka.produce_v0("xxx", 0, "v0")
#      }.to raise_error(Kafka::MessageNotFound)
    end
  end

  describe "#produce_v1" do
    # TODO: this test expects "tmp" topic exists
    it "returns Kafka::Message when exists" do
      mes = kafka.produce_v1("tmp", 0, "v1")
      expect(mes).to be_a(Kafka::Protocol::ProduceV1Response)
    end

    it "raises not found exception when missing" do
#      expect{
#        mes = kafka.produce_v1("xxx", 0, "v1")
#      }.to raise_error(Kafka::MessageNotFound)
    end
  end
end
