require "./spec_helper"

include Kafka::Protocol::Structure

describe Kafka::Protocol::Structure::ListOffsetsResponseV0::PartitionOffset do
  subject { ListOffsetsResponseV0::PartitionOffset.new(0, 0_i16, offsets.map(&.to_i64)) }

  describe "[]" do
    let(offsets) { [] of Int32 }

    it "#count, #offset" do
      expect(subject.count).to eq(0)
      expect(subject.offset).to eq(0)
    end
  end

  describe "[0]" do
    let(offsets) { [0] }

    it "#count, #offset" do
      expect(subject.count).to eq(0)
      expect(subject.offset).to eq(0)
    end
  end

  describe "[436, 0]" do
    let(offsets) { [436, 0] }

    it "#count, #offset" do
      expect(subject.count).to eq(436)
      expect(subject.offset).to eq(436)
    end
  end

  describe "[97, 96, 94]" do
    let(offsets) { [97, 96, 94] }

    it "#count, #offset" do
      expect(subject.count).to eq(3)
      expect(subject.offset).to eq(97)
    end
  end
end
