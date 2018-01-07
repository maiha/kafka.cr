require "./spec_helper"

describe Kafka::Commands::Topics do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  describe "#topics" do
    it "returns all topics except consumer_offsets when empty args are given" do
      topics = kafka.topics
      expect(topics).to be_a(Array(Kafka::TopicInfo))
      expect(topics.count{|i| i.name == "__consumer_offsets"}).to eq(0)
    end

    it "returns all topics with consumer_offsets when consumer_offsets option is given" do
      topics = kafka.topics(consumer_offsets: true)
      expect(topics).to be_a(Array(Kafka::TopicInfo))
    end
  end
end
