module Ping
  class Command
    getter :host, :port, :no, :guess_version, :last_result
    getter :req_seq, :msec, :started_at, :code, :state

    def initialize(@host, @port, @no, @guess_version, @last_result)
      # ## essential variables (used by finalizer)
      @code = Result::Code::ER
      @state = "(not executed yet)"
      @req_seq = "(#{no})"
      @msec = "(not executed yet)"
      @started_at = Time.now
    end

    private def state_changed?
      last_state && last_state != state
    end

    private def last_state
      last_result.try(&.state)
    end

    def run : Result
      req = Kafka::Protocol::HeartbeatRequest.new
      req.client_id = "kafka-ping"
      req.correlation_id = no

      socket = TCPSocket.new host, port
      socket.write(req.to_binary)
      socket.flush

      res = Kafka::Protocol::HeartbeatResponse.from_io(socket)
      req_seq = "#{res.correlation_id}"

      @state = "errno=#{res.error_code}"
      if guess_version
        case res.error_code
        when -1
          @state = "(0.8.x)"
        when 16
          @state = "(0.9.x)"
        end
      end

      return Result.new(Result::Code::OK, @state)

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

      STDOUT.puts "[#{time}] #{state} from #{host}:#{port} req_seq=#{req_seq} time=#{msec} ms"

      if state_changed?
        STDERR.puts "[#{time}] #{host}:#{port} : #{last_state} -> #{state}"
        STDERR.flush
      end
      
      socket.try &.close
    end
  end
end
