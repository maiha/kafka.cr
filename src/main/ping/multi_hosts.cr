class Ping::MultiHosts
  def initialize(@dests : Array(Dest))
    @stats = Utils::EnumStatistics(Result::Code).new
  end

  def run
    puts "ERROR: sorry, multi-hosts monitoring is not implemented yet!"
  end
end
