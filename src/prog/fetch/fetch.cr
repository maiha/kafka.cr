require "../app"

class Fetch < App
  include Options
  include Utils::GuessBinary
  
  option partition : Int32, "-p NUM", "--partition NUM", "Specify partition number", 0
  option max_wait : Int32, "--max-wait MSEC", "The max wait time is the maximum amount of time in milliseconds to block waiting", 1000
  option min_bytes : Int32, "--max-bytes SIZE", "This is the minimum number of bytes of messages", 0
  option strict : Bool, "--strict", "Do not discover leader broker automatically", false
  options :broker, :topic, :offset, :max_bytes, :guess, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topic]

Options:

Example:
  #{$0} topic1
  #{$0} topic1 -g  # to guess binary for like MessagePack
EOF

  def do_show(topic)
    req = build_fetch_request(topic)
    res = execute(req)
    res = execute(req, resolve_leader!(topic, partition)) if not_reader?(res) && !strict
    print_res(res)
  end
 
  private def build_fetch_request(topic)
    replica = -1
    ps = Kafka::Protocol::Structure::FetchRequestPartitions.new(partition, offset, max_bytes)
    ts = [Kafka::Protocol::Structure::FetchRequestTopics.new(topic, [ps])]
    return Kafka::Protocol::FetchRequest.new(0, app_name, replica, max_wait, min_bytes, ts)
  end

  private def not_reader?(res)
    res.topics.each do |t|
      t.partitions.each do |p|
        return true if p.error_code == 6
      end
    end
    return false
  end
  
  private def print_res(res)
    res.topics.each do |t|
      t.partitions.each do |p|
        head = "#{t.topic}##{p.partition}"
        if p.error_code == 0
          p.message_sets.each do |m|
            bytes = m.message.value
            value = guess ? guess_binary(bytes) : bytes
            STDOUT.puts "#{head}\t#{m.offset}: #{value.to_s}"
          end
        else
          errmsg = Kafka::Protocol.errmsg(p.error_code)
          STDOUT.puts "#{head}\t#{errmsg}".colorize(:red)
        end
      end
    end
  end

  def execute
    topics = ([topic] + args).reject(&.empty?).uniq

    case topics.size
    when 0
      die "no topics"
    when 1
      return do_show(topics.first.not_nil!)
    else
      die "please specify one topic"
    end
  end
end
