require "option_parser"
require "socket"
require "./kafka"

class Metadata::Main
  getter :args, :topic, :nop, :dump, :usage, :help

  def initialize(@args)
    @nop = false
    @dump = false
    @usage = false
    @topic = ""

    opts = OptionParser.parse(args) do |parser|
      parser.on("-d", "--dump", "Dump octal data") { @dump = true }
      parser.on("-n", "--nop", "Show request data") { @nop = true }
      parser.on("-t TOPIC", "--topic=TOPIC", "The topic to get metadata") { |t| @topic = t }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      puts "Usage: kafka-metadata [options] destination"
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

    topics = [topic].reject(&.empty?)
    req = Kafka::Protocol::MetadataRequest.new(0, "kafak-metadata", topics)
    bytes = req.to_slice

    if nop
      if dump
        p bytes
      else
        p req
      end
      exit
    end

    broker = Kafka::Cluster::Broker.parse(args.shift.not_nil!)
    socket = TCPSocket.new broker.host, broker.port

    spawn do
      socket.write bytes
      socket.flush
      sleep 0
    end

    if dump
      p Kafka::Protocol.read(socket)
    else
      puts Kafka::Protocol::MetadataResponse.from_kafka(socket).to_s
    end

    socket.close
  end
end

module Metadata
  def self.run(args)
    Main.new(args).run
  end
end
