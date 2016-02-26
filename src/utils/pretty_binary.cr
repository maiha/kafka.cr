module Utils::PrettyBinary
  def pretty_binary(bytes : Slice(UInt8)) : String
    s = String.new(bytes)
    s.inspect
    return s.inspect
  rescue
    "(binary)#{bytes.inspect}"
  end

  def pretty_binary(line : String) : String
    line.gsub(/\[((\d+)\s*(,\s*\d+)*)\]/) {
      orig = $1
      ary = orig.split(/\s*,\s*/).map(&.to_u8)
      if ary.all?{|i| 0 <= i < 256}
        s = Slice.new(ary.size) {|i| ary[i]}
        pretty_binary(s)
      else
        orig
      end
    }
  end
end
