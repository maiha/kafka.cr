require "option_parser"
require "socket"

class Ping::Main
  getter :count, :usage, :guess, :dests, :help

  @dests : Array(Kafka::Broker)
  @help : Proc(NoReturn)

  def initialize(args)
    @count = 86400
    @usage = false
    @guess = false

    opts = OptionParser.parse(args) do |parser|
      parser.on("-c NUM", "--count=NUM", "Stop after sending count requests") { |num| count = num.to_i }
      parser.on("-g", "--guess", "Guess kafka version rather than errno") { @guess = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @dests = args.map{|s| Kafka::Broker.parse(s)}.not_nil!
    
    @help = -> {
      puts "Usage: kafka-ping [options] destination(s)"
      puts "  A destination is `host:port` or `host` (default port: 9092)"
      puts ""
      puts "Options:"
      puts opts
      puts "\n"
      puts <<-EXAMPLE
        Example:
          (single-host monitoring mode)
            #{PROGRAM_NAME} localhost
            #{PROGRAM_NAME} localhost:9092
            #{PROGRAM_NAME} -g localhost > ping.log 2> changed.log &

          (multi-hosts monitoring mode)
            #{PROGRAM_NAME} localhost localhost:9093 localhost:9094
        EXAMPLE
      exit
    }
  end

  def run
    help.call if usage
  
    case dests.size
    when 0
      help.call
    when 1
      Ping::SingleHost.new(dests.first.not_nil!, count, guess).run
    else
      Ping::MultiHosts.new(dests).run
    end
  end
end
