require "./spec_helper"

describe "(customer: init_producer_id)" do
  subject!(kafka) { Kafka.new(kafka_broker) }
  after { kafka.close }

  describe "#init_producer_id" do
    it "works with nil" do
      res = kafka.init_producer_id("", 3000)
      expect(res.error?).to eq(false)
    end

    it "causes error when unknown transaction id is given" do
      res = kafka.init_producer_id("hello", 3000)
      expect(res.error?).to eq(true)
    end
  end
end
