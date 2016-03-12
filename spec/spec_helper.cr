require "spec"
require "../src/kafka"

macro bytes(*array)
  Slice.new({{array.size}}) {|i| {{array}}[i].to_u8}
end
