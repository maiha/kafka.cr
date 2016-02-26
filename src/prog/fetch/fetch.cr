require "../app"

class Fetch < App
  include Options
  include Utils::GuessBinary
  
  option max_wait : Int32, "--max-wait MSEC", "The max wait time is the maximum amount of time in milliseconds to block waiting", 1000
  option min_bytes : Int32, "--max-bytes SIZE", "This is the minimum number of bytes of messages", 0
  option last : Bool, "-l", "--last", "Show last message", false
  option strict : Bool, "--strict", "Do not discover leader broker automatically", false
  options :broker, :partition, :topic, :offset, :max_bytes, :guess, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topic]

Options:

Example:
  #{$0} topic1
  #{$0} topic1 -g  # to guess binary for like MessagePack
  #{$0} topic1 -g --last
EOF

  def do_show(topic)
    req = build_fetch_request(topic)
    res = execute(req)
    res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
    print_res(res, guess)
  end
 
  def do_last(topic)

  end
  
  private def build_fetch_request(topic)
    replica = -1
    ps = Kafka::Protocol::Structure::FetchRequestPartitions.new(partition, offset, max_bytes)
    ts = [Kafka::Protocol::Structure::FetchRequestTopics.new(topic, [ps])]
    return Kafka::Protocol::FetchRequest.new(0, app_name, replica, max_wait, min_bytes, ts)
  end

  def execute
    topics = ([topic] + args).reject(&.empty?).uniq

    case topics.size
    when 0
      die "no topics"
    when 1
      topic = topics.first.not_nil!
      if last
        do_last(topic)
      else
        do_show(topic)
      end
    else
      die "please specify one topic"
    end
  end
end
