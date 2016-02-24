require "../app"

class Info < App
  include Options
  include Utils::GuessBinary
  include Kafka::Protocol::Structure
  
  option before : Int64, "--before MSEC", "Used to ask for all messages before a certain time (ms)", -1
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

  private def fetch_offsets(meta : Kafka::Protocol::MetadataResponse, latest_offset : Int64)
    maps = meta.broker_maps
    broker = ->(id : Int32) {maps[id] || raise "[BUG] broker(#{id}) not found: meta=#{meta.brokers.inspect}"}

    builder = Builder::LeaderBasedOffsetRequestsBuilder.new(meta)
    builder.latest_offset = latest_offset
    reqs = builder.build
    
    chan = Channel(Kafka::Protocol::OffsetResponse).new
    reqs.each do |leader, req|
      spawn {
        req = Kafka::Protocol::OffsetRequest.new(0, "kafka-info", -1, req.topic_partitions)
        res = execute(req, broker.call(leader))
        chan.send(res)
      }
    end

    return (1..reqs.size).map{|_| chan.receive}
  end
  
  def do_show(topics)
    meta = fetch_topic_metadata(topics)
    ress = fetch_offsets(meta, before.to_i64)

    ress.each do |res|
      print_offset_res(res)
    end
  end

  private def print_offset_res(res)
    res.topic_partition_offsets.each do |meta|
      meta.partition_offsets.each do |po|
        if po.error_code == 0
          puts "#{meta.topic}##{po.partition}\tcount=#{po.count} #{po.offsets.inspect}"
        else
          errmsg = Kafka::Protocol.errmsg(po.error_code)
          puts "#{meta.topic}##{po.partition}\t#{errmsg}"
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
