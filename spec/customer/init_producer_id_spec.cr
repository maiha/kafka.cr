require "./spec_helper"

describe "(customer: init_producer_id)" do
  subject!(kafka) { Kafka.new("kafka") }
  after { kafka.close }

  describe "#init_producer_id" do
    it "works with nil" do
      res = kafka.init_producer_id("", 3000)
      expect(res.error?).to eq(false)
      expect(res.producer_id).to eq 0
      expect(res.producer_epoch).to eq 0
    end

    it "works with string" do
      res = kafka.init_producer_id("hello", 3000)
      expect(res.error?).to eq(false)
      expect(res.producer_epoch).to eq -1
    end
  end
end
