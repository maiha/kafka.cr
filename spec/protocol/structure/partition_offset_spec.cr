require "./spec_helper"

include Kafka::Protocol::Structure

describe Kafka::Protocol::Structure::PartitionOffset do
  describe "count" do
    testcases = {
      [] of Int32  => 0,  # I don't know whether this case will happen
      [0]          => 0,
      [436, 0]     => 436,
      [97, 96, 94] => 3,
    }

    testcases.each do |offsets, expected|
      it "parse #{offsets.inspect} to #{expected}" do
        PartitionOffset.new(0, 0_i16, offsets.map(&.to_i64)).count.should eq(expected)
      end
    end
  end
end
