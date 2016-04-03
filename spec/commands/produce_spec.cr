require "./spec_helper"

describe Kafka::Commands::Produce do
  include Kafka::Commands::Produce

  describe ProduceOption do
    describe ".default" do
      subject { ProduceOption.default }
      it "version is 0" do
        expect(subject.version).to eq(0)
      end
    end
  end
end
