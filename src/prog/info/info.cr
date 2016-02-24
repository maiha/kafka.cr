require "../app"

class Info < App
  include Options
  include Utils::GuessBinary
  include Kafka::Protocol::Structure
  
  options :broker, :topic, :verbose, :version, :help

  usage <<-EOF
Usage: kafka-info [options] [topics]

Options:

Example:
  #{$0} topic1
EOF

  private def app_name
    "kafka-info"
  end
  
  def do_list
    do_show([] of String)
  end

  private def fetch_topic_metadata(topics)
    req = Kafka::Protocol::MetadataRequest.new(0, app_name, topics)
    return execute(req)
  end

  def do_show(topics)
    meta = fetch_topic_metadata(topics)
    reqs = meta.to_offset_requests
    maps = meta.broker_maps

    chan = Channel(String).new
    
    reqs.each do |leader, req|
      spawn {
        req = Kafka::Protocol::OffsetRequest.new(0, "kafka-info", -1, req.topic_partitions)
        broker = maps[leader] || raise "[BUG] broker(#{leader}) not found: meta=#{meta.brokers.inspect}"
        res = execute(req, broker)
        write_offset_res(res, chan)
      }
    end

    reqs.size.times do
      puts chan.receive
    end
  end

  private def write_offset_res(res, chan)
    res.topic_partition_offsets.each do |meta|
      meta.partition_offsets.each do |po|
        if po.error_code == 0
          chan.send "#{meta.topic}##{po.partition}\t#{po.offsets.inspect}"
        else
          errmsg = Kafka::Protocol.errmsg(po.error_code)
          chan.send "#{meta.topic}##{po.partition}\t#{errmsg}"
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
