require "../app"

class Fetch < App
  include Options
  
  option max_wait : Int32, "--max-wait MSEC", "The max wait time is the maximum amount of time in milliseconds to block waiting", 1000
  option min_bytes : Int32, "--min-bytes SIZE", "This is the minimum number of bytes of messages", 0
  option last : Bool, "-l", "--last", "Show last message", false
  option strict : Bool, "--strict", "Do not discover leader broker automatically", false
  options :broker, :partition, :topic, :offset, :max_bytes, :raw, :guess, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topic]

Options:

Example:
  #{$0} topic1
  #{$0} topic1 -g  # to guess binary for like MessagePack
  #{$0} topic1 -g --last
EOF

  private def fetch(topic, offset : Int64)
    req = build_fetch_request(topic, offset)
    res = execute(req)
    res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
    return res
  end

  private def format
    return :RAW if raw
    return :GUESS if guess
    return true
  end
  
  def do_show(topic, offset)
    res = fetch(topic, offset)
    print_res(res, format)
  end
 
  def do_last(topic)
    req = build_offset_request([topic], partition)
    res = execute(req)
    res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
    offset = extract_last_offset!(res, topic)
    do_show(topic, offset)
  end

  private def extract_last_offset!(res, topic)
    res.topic_partition_offsets.each do |tpo|
      next unless tpo.topic == topic
      tpo.partition_offsets.each do |po|
        if po.count > 0
          return po.offset - 1
        else
          break
        end
      end
    end
    die "no offset found for #{topic}", false
  end
  
  private def build_fetch_request(topic, offset : Int64)
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
        do_show(topic, offset)
      end
    else
      die "please specify one topic"
    end
  end
end
