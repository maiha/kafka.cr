require "option_parser"
require "socket"
require "./kafka"

class Heartbeat::Main
  getter :args, :dump, :usage, :help

  def initialize(@args)
    @dump = false
    @usage = false

    opts = OptionParser.parse(args) do |parser|
      parser.on("-d", "--dump", "Dump octal data") { @dump = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      puts "Usage: kafka-heartbeat [options] destination"
      puts "  A destination is `host:port` or `host` (default port: 9092)"
      puts ""
      puts "Options:"
      puts opts
      puts "\n"
      puts <<-EXAMPLE
        Example:
          #{$0} localhost
          #{$0} localhost:9092
          #{$0} -d localhost
        EXAMPLE
      exit
    }
  end

  def run
    if usage || args.empty?
      help.call
    end

    broker = Kafka::Cluster::Broker.parse(args.shift.not_nil!)
    socket = TCPSocket.new broker.host, broker.port

    req = Kafka::Protocol::HeartbeatRequest.new(0, "kafka-heartbeart", "x", -1, "cr")
    bytes = req.to_slice

    spawn do
      socket.write bytes
      socket.flush
      sleep 0
    end

    if dump
      p Kafka::Protocol.read(socket)
    else
#      p Kafka::Protocol::HeartbeatResponse.from_io(socket)
    end
  end
end

module Heartbeat
  def self.run(args)
    Main.new(args).run
  end
end
