require "./spec_helper"

describe Kafka::Commands::Produce do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  # [prepare]
  # kafka-topics.sh --zookeeper localhost:2181 --create --topic 'tmp' --replication-factor=1 --partitions 1

  describe "#produce_v0" do
    # TODO: this test expects "tmp" topic exists
    it "returns Kafka::Protocol::ProduceV0Response when topic exists (tmp,0)" do
      mes = kafka.produce_v0("tmp", 0, "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceV0Response)
      expect(mes.error?).to eq(false)
    end

    it "returns error?=true when topic is missing" do
      mes = kafka.produce_v0("_tmp", 0, "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceV0Response)
      expect(mes.error?).to eq(true)
    end
  end

  describe "#produce_v1" do
    # TODO: this test expects "tmp" topic exists
    it "returns Kafka::Protocol::ProduceV1Response when topic exists (tmp,0)" do
      mes = kafka.produce_v1("tmp", 0, "v1")
      expect(mes).to be_a(Kafka::Protocol::ProduceV1Response)
      expect(mes.error?).to eq(false)
    end

    it "returns error?=true when topic is missing" do
      mes = kafka.produce_v1("_tmp", 0, "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceV1Response)
      expect(mes.error?).to eq(true)
    end
  end
end
