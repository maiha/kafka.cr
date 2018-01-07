require "../app"

class ClusterWatch < App
  include Options
    
  option interval : Int32, "-i SECONDS", "--interval=SECONDS", "Specify update interval seconds.", 60
  option colorize : Bool, "-c", "--color", "Colorize the output.", false
  options :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options]  broker(s)

Options:

Example:
  #{PROGRAM_NAME} localhost:9092
  #{PROGRAM_NAME} localhost:9092 -i 10
  #{PROGRAM_NAME} localhost:9092 localhost:9093 > cluster.log 2> changed.log
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

    @last_result : ClusterWatch::Result?

    record Setting,
      colorize : Bool
    
    def initialize(@host : String, @port : Int32, @expected_count : Int32, @last_result : ClusterWatch::Result?, @out : Channel::Unbuffered(String), @err : Channel::Unbuffered(String), @setting : ClusterWatch::Command::Setting )
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

      @code = (res.brokers.size == @expected_count) ? Result::Code::OK : Result::Code::KO
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
      mark = ok? ? "OK: " : "NG: "
      
      @out.send colorize("#{mark}[#{time}] #{state} from #{host}:#{port} time=#{msec} ms")

      if state_changed?
        @err.send colorize("#{mark}[#{time}] #{host}:#{port} : #{last_state} -> #{state}")
      end
      socket.try &.close
    end

    private def ok?
      @code == Result::Code::OK
    end

    private def colorize(msg : String) : String
      unless @setting.colorize
        return msg
      end
      
      if ok?
        msg.colorize(:green).to_s
      else
        msg.colorize(:yellow).to_s
      end
    end
  end

  class BrokerWatcher
    delegate host, port, to: @dest

    getter :interval, :stats, :no

    @last_result : ClusterWatch::Result?

    def initialize(@dest : Kafka::Broker, @broker_count : Int32, @colorize : Bool)
      @stats = Utils::EnumStatistics(Result::Code).new
      @ch  = Channel(Result).new
      @out = Channel(String).new
      @err = Channel(String).new
      @command_setting = Command::Setting.new(@colorize)
    end

    def start(interval)
      @last_result = nil
      spawn do
        loop do
          sleep interval
          @last_result = Command.new(host, port, @broker_count, @last_result, @out, @err, @command_setting).execute
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
    watchers = args.map{|b| BrokerWatcher.new(Kafka::Broker.parse(b), args.size, colorize) }
    if watchers.empty?
      die "no brokers"
    end

    register_shutdown_hook(watchers)
    main_loop(watchers)
  end

  private def main_loop(watchers)
    channel3s = watchers.map(&.start(interval.seconds))

    loop do
      channel3s.each do |(ch1, ch2, ch3)|
        select
        when value = ch1.receive
        when value = ch2.receive
          logger.info value
        when value = ch3.receive
          logger.error value
          STDERR.flush
        end
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
