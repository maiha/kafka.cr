require "./spec_helper"

describe "(customer: produce)" do
  subject!(kafka) { Kafka.new(kafka_broker) }
  after { kafka.close }

  describe "#produce(v0)" do
    it "returns error?=true when topic is missing" do
      mes = kafka.produce("_tmp", "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV0)
      expect(mes.error?).to eq(true)
    end

    it "accepts string" do
      mes = kafka.produce("test", "v0")
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV0)
      expect(mes.error?).to eq(false)
    end

    it "accepts strings" do
      mes = kafka.produce("test", ["v0#1", "v0#2"])
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV0)
      expect(mes.error?).to eq(false)
    end

    it "accepts binary" do
      mes = kafka.produce("test", bytes(0,1,2))
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV0)
      expect(mes.error?).to eq(false)
    end

    it "accepts binaries" do
      mes = kafka.produce("test", [bytes(0,1,2), bytes(3,4)])
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV0)
      expect(mes.error?).to eq(false)
    end
  end

  describe "#produce(v1)" do
    it "returns error?=true when topic is missing" do
      mes = kafka.produce("_tmp", "v1", version: 1)
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV1)
      expect(mes.error?).to eq(true)
    end

    it "accepts string" do
      mes = kafka.produce("test", "v1", version: 1)
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV1)
      expect(mes.error?).to eq(false)
    end

    it "accepts strings" do
      mes = kafka.produce("test", ["v1#1", "v1#2"], version: 1)
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV1)
      expect(mes.error?).to eq(false)
    end

    it "accepts binary" do
      mes = kafka.produce("test", bytes(0,1,2), version: 1)
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV1)
      expect(mes.error?).to eq(false)
    end

    it "accepts binaries" do
      mes = kafka.produce("test", [bytes(0,1,2), bytes(3,4)], version: 1)
      expect(mes).to be_a(Kafka::Protocol::ProduceResponseV1)
      expect(mes.error?).to eq(false)
    end
  end

  describe "#produce(v2)" do
    it "not implemented yet" do
      expect {
        kafka.produce("test", "v2", version: 2)
      }.to raise_error Kafka::NotImplemented
    end
  end

  describe "#produce(v3)" do
    pending "accepts string" do
      msg = kafka.produce("test", "v3", version: 3)
      expect(msg).to be_a(Kafka::Protocol::ProduceResponseV3)
      expect(msg.error?).to eq(false)
    end
  end

  describe "#produce(v5)" do
    pending "accepts string" do
      msg = kafka.produce("test", "v5", version: 5)
      expect(msg).to be_a(Kafka::Protocol::ProduceResponseV5)
      expect(msg.error?).to eq(false)
    end
  end
end
