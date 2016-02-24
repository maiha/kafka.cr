require "../app"

class Info < App
  include Options
  include Utils::GuessBinary
  include Kafka::Protocol::Structure

  record TopicDayCount, topic, day, count
  
  option before : Int64, "--before MSEC", "Used to ask for all messages before a certain time (ms)", -1
  option days : Int32, "--days NUM", "Show histogram publish counts for NUM days (causes NUM times reqs to kafka)", 0
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
  
  def do_show(topics, time)
    meta = fetch_topic_metadata(topics)
    ress = fetch_offsets(meta, time)

    ress.each do |res|
      print_offset_res(res)
    end
  end

  def do_histogram_days(topics, num)
    meta = fetch_topic_metadata(topics)

    # now = Time.now.epoch_ms
    today = Time.now.at_end_of_day # midnight of today
    records = (0..num).map{|n|
      ress = fetch_offsets(meta, (today - n.days).epoch_ms)
      ress.map{|res| extract_topic_day_counts(res, n)}
    }.flatten

    grouped = records.group_by{|r| [r.topic, r.day]}.map{|key, ary|
      t = key[0].not_nil!
      d = key[1].not_nil!.to_i
      TopicDayCount.new(t, d, ary.sum(&.count))
    }
    sorted = grouped.sort_by{|r| "%s %04d" % [r.topic, r.day]}

    sorted.group_by(&.topic).each do |topic, records|
      puts "[#{topic}]"
      prev = 0
      records.reverse.each_with_index do |r, i|
        diff = (i == 0) ? 0 : r.count - prev
        prev = r.count
        date = (today - r.day.days).to_s("%Y-%m-%d")
        if diff > 0
          puts "%s %d (+%d)" % [date, r.count, diff]
        else
          puts "%s %d" % [date, r.count]
        end
      end
      puts ""
    end
  end

  def execute
    topics = ([topic] + args).reject(&.empty?).uniq

    if topics.any?
      if days > 0
        return do_histogram_days(topics, days)
      else
        return do_show(topics, before.to_i64)
      end
    end

    die "no topics"
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

  private def extract_topic_day_counts(res, day)
    res.topic_partition_offsets.map{ |meta|
      meta.partition_offsets.map{|po|
        TopicDayCount.new(meta.topic, day, po.count)
      }
    }
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
end
