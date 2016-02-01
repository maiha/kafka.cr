require "option_parser"
require "socket"
require "../kafka"

class Topics::Main
  getter :args, :list, :topic, :broker, :verbose, :usage, :help

  def initialize(@args)
    @usage = false
    @list  = false
    @verbose = false
    @topic = ""
    @broker = "127.0.0.1:9092"

    opts = OptionParser.parse(args) do |parser|
      parser.on("-b URL", "--broker=URL", "The connection string for broker (default: 127.0.0.1:9092)") { |b| @broker = b }
      parser.on("-l", "--list", "List all available topics") { @list = true }
      parser.on("-t TOPIC", "--topic=TOPIC", "The topic to get metadata") { |t| @topic = t }
      parser.on("-v", "--verbose", "Verbose output") { @verbose = true }
      parser.on("-h", "--help", "Show this help") { @usage = true }
    end

    @help = -> {
      STDERR.puts "Usage: kafka-topics [options] [topics]"
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
    req = Kafka::Protocol::MetadataRequest.new(0, "kafak-topics", topics)
    res = execute req
    res.topics.each do |meta|
      msg = verbose ? meta.to_s : meta.name
      if meta.error_code == 0
        STDOUT.puts msg
      else
        STDERR.puts "ERROR: #{msg}"
      end
    end
  end

  def run
    if usage
      help.call
    end

    if list
      if topic_given? || args.any?
        die "Both `--list' and `topic' exist. Specify one of them."
      end
      return do_list
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
      return Kafka::Protocol::MetadataResponse.from_kafka(socket)
    end
  end

end

module Topics
  def self.run(args)
    Main.new(args).run
  ensure
    STDOUT.flush
    STDERR.flush
  end
end
