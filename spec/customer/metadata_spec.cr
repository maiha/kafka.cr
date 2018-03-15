require "./spec_helper"

describe "(customer: metadata)" do
  subject!(kafka) { Kafka.new(kafka_broker) }
  after { kafka.close }

  describe "#metadata" do
    it "returns metadata information" do
      info = kafka.metadata(["t1"])
      expect(info).to be_a(Kafka::MetadataInfo)
    end

    it "contains brokers" do
      info = kafka.metadata(["t1"])
      expect(info.brokers).to be_a(Array(Kafka::Broker))
    end

    it "contains topics" do
      info = kafka.metadata(["t1"])
      expect(info.topics).to be_a(Array(Kafka::TopicInfo | Kafka::TopicError))
    end
  end

  describe "#raw_metadata" do
    it "returns metadata response object" do
      res = kafka.raw_metadata(["t1"])
      expect(res).to be_a(Kafka::Protocol::MetadataResponseV0)
    end

    it "contains brokers" do
      res = kafka.raw_metadata(["t1"])
      expect(res.brokers).to be_a(Array(Kafka::Protocol::Structure::Broker))
    end

    it "contains topics" do
      res = kafka.raw_metadata(["t1"])
      expect(res.topics).to be_a(Array(Kafka::Protocol::Structure::MetadataResponseV0::TopicMetadata))
    end
  end
end
