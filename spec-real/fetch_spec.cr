require "./spec_helper"

describe Kafka::Commands::Fetch do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  describe "#fetch" do
    # TODO: this test expects "t1" topic exists
    it "fetch value" do
#      kafka.handler.request = ->(req: Kafka::Request) { p req }
#      kafka.handler.respond = ->(res: Kafka::Response) { p res }
      kafka.fetch("t1", 0, 0_i64)
    end
  end
end
