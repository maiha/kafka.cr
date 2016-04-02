require "./spec_helper"

describe Utils::GuessBinary do
  include Utils::GuessBinary

  describe "guess_binary" do
    it "Msgpack" do
      expect(guess_binary([1,2].to_msgpack)).to be_a(Msgpack)
    end

    it "Null" do
      expect(guess_binary(Slice(UInt8).new(0))).to be_a(Null)
    end

    it "Unknown" do
      expect(guess_binary(bytes(0,1,2))).to be_a(Unknown)
    end

#    it "Text" do
#      expect(guess_binary(bytes(48,49))).to be_a(Text)
#    end
  end
end
