class Utils::EnumStatistics(T)
  delegate :size, :[], to: @counts

  def initialize
    @counts = Hash(T, Int32).new
    clear
  end

  def <<(key)
    @counts[key] += 1
  end

  def clear
    T.values.each { |v| @counts[v] = 0 }
  end

  def sum
    @counts.values.sum
  end

  def to_s
    T.values.map { |v| "#{v}=#{@counts[v]}" }.join(", ")
  end

  #  macro method_missing(name, arg, block)
  #    @counts[T.from_value({{name.id.stringify.upcase}})].not_nil!
  #  end
end
