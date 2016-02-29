require "../app"
require "http/server"

class Httpd < App
  include Options

  option host : String, "-h HOST", "--host HOST", "Specify listen host name. (default: '0.0.0.0')", "0.0.0.0"
  option port : Int32, "-p PORT", "--port PORT", "Specify listen port number. (default: 9000)", 9000
  options :broker, :topic, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topic]
  'GET /(TOPIC_NAME).json' returns last data.
  Available topics are filtered by args. (for security reason)

Options:

Example:
  #{$0} topic1
EOF

  def execute
    topics = ([topic] + args).reject(&.empty?).uniq

    case topics.size
    when 0
      die "no topics"
    else
      start_httpd(host, port, broker, topics)
    end
  end

  private def execute(broker, topic, opts = "") : String
    cmd = "kafka-fetch -b #{broker.to_s} #{topic} -l -g #{opts}"
    line = `#{cmd}`
    # TODO: use inner program
    # TODO: error handling
    return line
  end

  private def start_httpd(host, port, broker, valid_topics)
    server = HTTP::Server.new(host, port) do |context|
      log_request(context)
      case context.request.path
      when %r{\A/([a-z0-9_.]+)\.json\Z}
        topic = $1
        if valid_topics.includes?(topic)
          do_process_request(context, broker, topic)
        else
          do_invalid_request(context)
        end
      else
        do_invalid_request(context, 404, "NOT FOUND")
      end
    end
    server.listen
  end

  private def log_request(context)
    puts "#{Time.now} #{context.request.inspect}"
  end

  private def respond_json(context, code, body)
    context.response.headers["Content-Type"] = "application/json"
    context.response.status_code = code
    context.response.print({"code" => code, "body" => body}.to_json)
  end
  
  private def do_process_request(context, broker, topic)
    respond_json(context, 200, execute(broker, topic))
  rescue err
    respond_json(context, 500, "INTERNAL SERVER ERROR: #{err}")
  end

  private def do_invalid_request(context, code = 400, body = "invalid request")
    body = {"code" => code, "body" => body}.to_json

    context.response.headers["Content-Type"] = "application/json"
    context.response.status_code = code
    context.response.print(body)
  end
end
