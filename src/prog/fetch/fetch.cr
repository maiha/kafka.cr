require "../app"

class Fetch < App
  include Options
  include Utils::GuessBinary
  
  option partition : Int32, "-p NUM", "--partition NUM", "Specify partition number", 0
  option max_wait : Int32, "--max-wait MSEC", "The max wait time is the maximum amount of time in milliseconds to block waiting", 1000
  option min_bytes : Int32, "--max-bytes SIZE", "This is the minimum number of bytes of messages", 0
  options :broker, :topic, :offset, :max_bytes, :guess, :verbose, :version, :help

  usage <<-EOF
Usage: kafka-fetch [options] [topics]

Options:

Example:
  #{$0} topic1
  #{$0} topic1 topic2
  #{$0} topic1 -v
  #{$0} topic1 -g  # to guess binary for like MessagePack
EOF

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    replica = -1
    
    ps = Kafka::Protocol::Structure::FetchRequestPartitions.new(
      partition, offset, max_bytes
    )
    ts = topics.map{|t|
      Kafka::Protocol::Structure::FetchRequestTopics.new(t, [ps])
    }

    req = Kafka::Protocol::FetchRequest.new(0, "kafka-fetch", replica, max_wait, min_bytes, ts)
    res = execute req

    print_res(res)
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

    if topics.any?
      return do_show(topics)
    end

    die "no topics"
  end
end
