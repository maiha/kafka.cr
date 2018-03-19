require "./spec_helper"

describe "(customer: fetch)" do
  subject!(kafka) { Kafka.new(kafka_broker) }
  after { kafka.close }

  describe "#fetch(v0)" do
    it "raises protocol error for the missing topics" do
      expect {
        kafka.fetch("_tmp")
      }.to raise_error(Kafka::Protocol::Error)
    end

    it "returns Kafka::Message" do
      body = Time.now.to_s
      res = kafka.produce("test", body)
      mes = kafka.fetch("test")
      expect(mes).to be_a(Kafka::Message)
    end
  end
end
