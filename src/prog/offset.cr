require "option_parser"
require "socket"
require "../kafka"

class Offset::Main
  include Kafka::Protocol
  
  getter :args, :usage, :help, :list, :topic, :broker, :verbose
  
  def initialize(@args)
    @usage = false
    @list  = false
    @verbose = false
    @topic = ""
    @broker = "127.0.0.1:9092"

    opts = OptionParser.parse(args) do |parser|
      parser.on("-b URL", "--broker=URL", "The connection string for broker (default: 127.0.0.1:9092)") { |b| @broker = b }
      parser.on("-t TOPIC", "--topic=TOPIC", "The topic to get metadata") { |t| @topic = t }
      parser.on("-v", "--verbose", "Verbose output") { @verbose = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      STDERR.puts "Usage: kafka-offset [options] [topics]"
      STDERR.puts ""
      STDERR.puts "Options:"
      STDERR.puts opts
      STDERR.puts "\n"
      STDERR.puts <<-EXAMPLE
        Example:
          #{$0} --list --broker=localhost:9092
          #{$0} topic1
          #{$0} topic1 topic2
          #{$0} -v topic1 
        EXAMPLE
      exit
    }
  end

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    replica = -1
    partition = Structure::Partition.new(p = 0, latest_offset = -1_i64, max_offsets = 999999999)
    taps = topics.map{|t| Structure::TopicAndPartitions.new(t, [partition])}
    req = Kafka::Protocol::OffsetRequest.new(0, "kafka-offset", replica, taps)
    res = execute req
    res.topic_partition_offsets.each do |meta|
      meta.partition_offsets.each do |po|
        if po.error_code == 0
          puts "#{meta.topic}##{po.partition}\t#{po.offsets.inspect}"
        else
          errmsg = Kafka::Protocol.errmsg(po.error_code)
          puts "#{meta.topic}##{po.partition}\t#{errmsg}"
        end
      end
    end
  end

  def run
    if usage
      help.call
    end

    topics = ([topic] + args).reject(&.empty?)

    if topics.any?
      return do_show(topics)
    end

    die "no operations"

  rescue err : Errno
    die err
  end

  private def topic_given?
    !topic.empty?
  end

  private def die(msg)
    STDERR.puts "ERROR: #{msg}\n\n"
    help.call
  end

  private def build_broker
    Kafka::Cluster::Broker.parse(self.broker)
  end

  private def open
    broker = build_broker
    socket = TCPSocket.new broker.host, broker.port

    begin
      yield(socket)
    ensure
      socket.close
    end
  end

  private def execute(request)
    bytes = request.to_slice
    open do |socket|
      spawn do
        socket.write bytes
        socket.flush
        sleep 0
      end
      return Kafka::Protocol::OffsetResponse.from_kafka(socket, verbose)
    end
  end

end

module Offset
  def self.run(args)
    Main.new(args).run
  ensure
    STDOUT.flush
    STDERR.flush
  end
end
