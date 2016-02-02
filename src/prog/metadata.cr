require "option_parser"
require "socket"
require "../kafka"

module Metadata
end

class Metadata::Main
  getter :args, :usage, :verbose, :consumer, :all, :nop, :dump, :broker

  def initialize(@args)
    @usage = false
    @verbose = false
    @consumer = false
    @all = false
    @nop = false
    @dump = false
    @broker = "127.0.0.1:9092"
  end

  private def parse_opts
    opts = OptionParser.parse(args) do |parser|
      parser.on("-a", "--all", "Get all topics") { @all = true }
      parser.on("-b URL", "--broker=URL", "The connection string for broker (default: 127.0.0.1:9092)") { |b| @broker = b }
      parser.on("-c", "--consumer_offsets", "Show consumer offsets as topic") { @consumer = true }
      parser.on("-d", "--dump", "Dump octal data (Simulation mode)") { @dump = true }
      parser.on("-n", "--nop", "Show request data (Simulation mode)") { @nop = true }
      parser.on("-v", "--verbose", "Verbose output") { @verbose = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      puts "Usage: kafka-metadata [options] [topics]"
      puts ""
      puts "Options:"
      puts opts
      puts "\n"
      puts <<-EXAMPLE
        Example:
          #{$0} topic1
          #{$0} topic1 topic2
          #{$0} -a
          #{$0} -b localhost:9092 -a
        EXAMPLE
      exit
    }
  end


  def run
    parse_opts

    if usage
      help.call
    end

    topics = args.reject(&.empty?)

    if topics.empty? && !all
      die "no topics. specify topic name, or use `-a' to show all topics"
    end

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

    broker = build_broker
    socket = TCPSocket.new broker.host, broker.port

    spawn do
      socket.write bytes
      socket.flush
      sleep 0
    end

    if dump
      p Kafka::Protocol.read(socket)
    else
      res = Kafka::Protocol::MetadataResponse.from_kafka(socket, verbose)
      show_res(res)
    end

    socket.close

  rescue err
    die err
  end

  private def show_res(res)
    # show brokers
    res.brokers.each do |b|
      puts "broker##{b.node_id}\t#{b.host}:#{b.port}"
    end

    # show topics
    res.topics.each do |t|
      next if !consumer && t.name == "__consumer_offsets"
      if t.error_code == -1
        errmsg = Kafka::Protocol.errmsg(t.error_code)
        puts "#{t.name}\t#{errmsg}".colorize(:red)
      else
        t.partitions.each do |p|
          if p.error_code == -1
            errmsg = Kafka::Protocol.errmsg(p.error_code)
            STDERR.puts "#{t.name}##{p.id}\t#{errmsg}".colorize(:red)
            STDERR.flush
          else
            puts "#{t.name}##{p.id}\tleader=#{p.leader},replica=#{p.replicas.inspect},isr=#{p.isrs.inspect}"
          end
        end
      end
    end          
  end

  private def build_broker
    Kafka::Cluster::Broker.parse(self.broker)
  end

  private def help
    @help || raise "BUG: @help not defined yet"
  end

  private def die(msg)
    STDERR.puts "ERROR: #{msg}\n\n"
    STDERR.flush
    help.call
  end
end

module Metadata
  def self.run(args)
    Main.new(args).run
    STDOUT.flush
    STDERR.flush
  end
end
