require "../spec_helper"

macro u8(*array)
  Slice.new({{array.size}}) {|i| {{array}}[i].to_u8}
end
