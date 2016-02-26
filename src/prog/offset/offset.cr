require "../app"

class Offset < App
  include Options
    
  options :broker, :topic, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topics]

Options:

Example:
  #{$0} topic1
  #{$0} topic1 topic2
  #{$0} topic1 --broker=localhost:9092
  #{$0} topic1 -v
EOF

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    replica = -1
    partition = Kafka::Protocol::Structure::Partition.new(p = 0, latest_offset = -1_i64, max_offsets = 999999999)
    taps = topics.map{|t| Kafka::Protocol::Structure::TopicAndPartitions.new(t, [partition])}
    req = Kafka::Protocol::OffsetRequest.new(0, "kafka-offset", replica, taps)
    res = execute req
    print_res(res)
  end

  private def print_res(res)
    res.topic_partition_offsets.each do |meta|
      meta.partition_offsets.each do |po|
        if po.error_code == 0
          puts "#{meta.topic}##{po.partition}\t#{po.offsets.inspect}"
        else
          errmsg = Kafka::Protocol.errmsg(po.error_code)
          puts "#{meta.topic}##{po.partition}\t#{errmsg}"
        end
      end
    end
  end

  def execute
    topics = ([topic] + args).reject(&.empty?)

    if topics.any?
      return do_show(topics)
    end

    die "no topics"
  end
end
