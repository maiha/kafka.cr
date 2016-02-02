require "option_parser"
require "socket"
require "../kafka"

class Topics::Main
  getter :args, :verbose, :usage, :list, :create, :topic, :broker, :partitions, :replication_factor

  def initialize(@args)
    @usage = false
    @list  = false
    @create = false
    @verbose = false
    @partitions = 1
    @replication_factor = 1
    @topic = ""
    @broker = "127.0.0.1:9092"
  end
  
  private def parse_opts
    opts = OptionParser.parse(args) do |parser|
      parser.on("-b URL", "--broker=URL", "The connection string for broker (default: 127.0.0.1:9092)") { |b| @broker = b }
      parser.on("-c", "--create", "Create a new topic") { @create = true }
      parser.on("-l", "--list", "List all available topics") { @list = true }
      parser.on("-p NUM", "--partitions=NUM", "The number of partitions for the topic") { |p| @partitions = p }
      parser.on("-r NUM", "--replication-factor=NUM", "The number of replication factor for the topic") { |r| @replication_factor = r }
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
          #{$0} --create --partitions=1 --replication-factor=1 --topic=topic1
          #{$0} -c -p 1 -r 1 topic1  # same as above
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
      if meta.error_code == 0
        STDOUT.puts verbose ? meta.to_s : meta.name
      else
        STDERR.puts "ERROR: #{meta.to_s}"
      end
    end
  end

  def do_create(topic : String)
    die "Sorry, `create' is not implemented yet"
  end

  def run
    parse_opts

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

    if create
      case topics.size
      when 1 ; return do_create(topics.first.not_nil!)
      when 0 ; die "no topics. `create' needs one topic"
      else   ; die "too many topics. `create' needs one topic"
      end
    end

    if topics.any?
      return do_show(topics)
    end

    die "no topics"

  rescue err
    die err
  end

  private def topic_given?
    !topic.empty?
  end

  private def die(msg)
    STDERR.puts "ERROR: #{msg}\n\n"
    STDERR.flush
    help.call
  end

  private def build_broker
    Kafka::Cluster::Broker.parse(self.broker)
  end

  private def help
    @help || raise "BUG: @help not defined yet"
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
