require "../app"

class ClusterWatch < App
  include Options
    
  option interval : Int32, "-i SECONDS", "--interval=SECONDS", "Specify update interval seconds.", 60
  options :verbose, :version, :help

  usage <<-EOF
Usage: kafka-cluster-watch [options] broker(s)

Options:

Example:
  #{$0} localhost:9092
  #{$0} localhost:9092 -i 10
  #{$0} localhost:9092 localhost:9093 > cluster.log 2> changed.log
EOF

  class Result
    enum Code
      OK
      KO
      ER
    end

    getter :code, :state

    def initialize(@code : Code, @state : String)
    end
  end

  class Command
    getter :host, :port, :expected_count, :last_result
    getter :msec, :started_at, :code, :state

    def initialize(@host, @port, @expected_count, @last_result, @out, @err)
      # ## essential variables (used by finalizer)
      @code = Result::Code::ER
      @state = "(not executed yet)"
      @msec = "(not executed yet)"
      @started_at = Time.now
    end

    private def state_changed?
      last_state && last_state != state
    end

    private def last_state
      last_result.try(&.state)
    end

    private def build_state(res : Kafka::Protocol::MetadataResponse)
      hosts = res.brokers.sort(&.node_id).map{|b| "#{b.host}:#{b.port}"}
      "(#{hosts.size})#{hosts.inspect}"
    end
    
    def execute
      topics = ["_"]              # dummy to avoid all topics
      req = Kafka::Protocol::MetadataRequest.new(0, "kafka-cluster-watcher", topics)
      
      socket = TCPSocket.new host, port
      socket.write(req.to_slice)
      socket.flush

      res = req.class.response.from_kafka(socket)

      @state = build_state(res)

      code = (res.brokers.size == @expected_count) ? Result::Code::OK : Result::Code::KO
      return Result.new(code, @state)
    rescue err : Errno
      case err.message
      when /Connection refused/i
        # [expected error] not listen (server is down)
        # #<Errno:0x1755ea0 @message="Error connecting to 'localhost:9091': Connection refused", @cause=ni
        @state = "(broker is down)"
      else
        @state = "#{err.message}"
      end

      return Result.new(Result::Code::ER, @state)
    ensure
      now = Time.now
      msec = (now - started_at).total_milliseconds.to_s
      time = now.to_s

      @out.send "[#{time}] #{state} from #{host}:#{port} time=#{msec} ms"

      if state_changed?
        @err.send "[#{time}] #{host}:#{port} : #{last_state} -> #{state}"
      end
      socket.try &.close
    end
  end

  class BrokerWatcher
    delegate :host, @dest
    delegate :port, @dest

    getter :interval, :stats, :no

    def initialize(@dest : Kafka::Cluster::Broker, @broker_count : Int32)
      @stats = Utils::EnumStatistics(Result::Code).new
      @ch  = Channel(Result).new
      @out = Channel(String).new
      @err = Channel(String).new
    end

    def start(interval)
      @last_result = nil
      spawn do
        loop do
          sleep interval
          @last_result = Command.new(host, port, @broker_count, @last_result, @out, @err).execute
          @ch.send(@last_result.not_nil!)
          stats << @last_result.not_nil!.code
        end
      end
      return [@ch, @out, @err]
    end

    def report
      ok = stats[Result::Code::OK]
      ko = stats[Result::Code::KO]
      er = stats[Result::Code::ER]
      got = stats.sum
      puts ""
      puts "--- #{host}:#{port} kafka watch statistics ---"
      puts "#{got} received, ok: #{ok}, ko: #{ko}, error: #{er}"
    end
  end
  
  def execute
    watchers = args.map{|b| BrokerWatcher.new(Kafka::Cluster::Broker.parse(b), args.size) }
    if watchers.empty?
      die "no brokers"
    end

    register_shutdown_hook(watchers)
    main_loop(watchers)
  end

  private def main_loop(watchers)
    channels = watchers.map(&.start(interval.seconds)).flatten
    receives = channels.map(&.receive_op)

    loop do
      index, value = Channel.select(receives)
#      broker_index = (index / 3)
      case index % 3
      when 0
        result = value as Result # typeof(channels[index].receive)
      when 1
        STDOUT.puts value
      when 2
        STDERR.puts value
        STDERR.flush
      else
        raise "BUG: Channel.select returned invalid index: #{index}"
      end
    end
  end
  
  private def register_shutdown_hook(watchers)
    at_exit { report(watchers) }
    Signal::INT.trap { exit }
  end

  private def report(watchers)
    watchers.each do |watch|
      watch.report
    end
    STDOUT.flush
    STDERR.flush
  end
end
