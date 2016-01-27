require "option_parser"
require "../kafka"
require "socket"
require "./utils/*"
require "./ping/*"

include Kafka::Protocol

######################################################################
### args and usage

count = 86400
usage = false
guess = false

opts = OptionParser.parse(ARGV) do |parser|
  parser.on("-c NUM", "--count=NUM", "Stop after sending count requests") { |num| count = num.to_i }
  parser.on("-g", "--guess", "Guess kafka version rather than errno") { guess = true }
  parser.on("-h", "--help", "Show this help") { usage = true }
end

dests = ARGV.map{|s| Ping::Dest.parse(s)}

help = -> {
  puts "Usage: kafka-ping [options] destination(s)"
  puts "  A destination is `host:port` or `host` (default port: 9092)"
  puts ""
  puts "Options:"
  puts opts
  puts "\n"
  puts <<-EXAMPLE
    Example:
      (single-host monitoring mode)
        #{$0} localhost
        #{$0} localhost:9092
        #{$0} -g localhost > ping.log 2> changed.log &

      (multi-hosts monitoring mode)
        #{$0} localhost localhost:9093 localhost:9094
    EXAMPLE
  exit
}

if usage
  help.call
end
  
######################################################################
### main

case dests.size
when 0
  help.call
when 1
  Ping::SingleHost.new(dests.first.not_nil!, count, guess).run
else
  Ping::MultiHosts.new(dests).run
end

