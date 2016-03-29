require "./spec_helper"

describe Kafka::Commands::Metadata do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  describe "#raw_metadata" do
    it "returns metadata response" do
      res = kafka.raw_metadata(["t1"])
      expect(res).to be_a(Kafka::Protocol::MetadataResponse)
    end

    it "contains brokers" do
      res = kafka.raw_metadata(["t1"])
      expect(res.brokers).to be_a(Array(Kafka::Protocol::Structure::Broker))
    end

    it "contains topics" do
      res = kafka.raw_metadata(["t1"])
      expect(res.topics).to be_a(Array(Kafka::Protocol::Structure::TopicMetadata))
    end
  end
end
