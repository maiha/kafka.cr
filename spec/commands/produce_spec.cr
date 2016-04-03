require "./spec_helper"

describe Kafka::Commands::Produce do
  include Kafka::Commands::Produce

  describe ProduceOption do
    describe "(default)" do
      subject(opt) { ProduceOption.new }

      it "version is 0" do
        expect(opt.version).to eq(0)
      end

      it "partition is 0" do
        expect(opt.partition).to eq(0)
      end
    end
  end
end
