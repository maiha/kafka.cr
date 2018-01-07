class Ping::SingleHost
  delegate host, port, to: @dest

  getter count, guess, interval, stats, no, last_result

  @last_result : Ping::Result?
  @interval : Time::Span

  def initialize(@dest : Kafka::Broker, @count : Int32, @guess : Bool)
    @interval = 1.second
    @stats = Utils::EnumStatistics(Result::Code).new
    @no = 0
  end

  def run
    register_shutdown_hook

    puts "Kafka PING #{host}:#{port} (by HeartbeatRequest)"
    count.times do |i|
      spawn { ping(i) }
      sleep interval
    end
  end

  private def ping(i)
    @no = i + 1
    @last_result = Ping::Command.new(host, port, no, guess, last_result).run
    stats << last_result.not_nil!.code
  rescue err
    logger.error "[internal error] ping(#{i}): #{err}".colorize.red
  end
        
  private def register_shutdown_hook
    at_exit { report }
    Signal::INT.trap { exit }
  end

  def report
    ok = stats[Result::Code::OK]
    er = stats[Result::Code::ER]
    got = stats.sum
    puts ""
    puts "--- #{host}:#{port} kafka ping statistics ---"
    puts "#{no} requests transmitted, #{got} received, ok: #{ok}, error: #{er}"
  end
end
