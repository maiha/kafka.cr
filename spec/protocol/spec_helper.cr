require "../spec_helper"

macro bytes(*array)
  Slice.new({{array.size}}) {|i| {{array}}[i].to_u8}
end
