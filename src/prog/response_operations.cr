module ResponseOperations
  include Utils::GuessBinary

  record TopicCount, topic, count

  protected def not_leader?(res)
    res.topics.each do |t|
      t.partitions.each do |p|
        return true if p.error_code == 6
      end
    end
    return false
  end

  protected def not_leader?(res : Kafka::Protocol::OffsetResponse)
    res.topic_partition_offsets.each do |meta|
      meta.partition_offsets.each do |po|
        return true if po.error_code == 6
      end
    end
    return false
  end
  
  protected def print_res(res : Kafka::Protocol::FetchResponse, format)
    res.topics.each do |t|
      t.partitions.each do |p|
        head = "#{t.topic}##{p.partition}"
        if p.error_code == 0
          p.message_sets.each do |m|
            bytes = m.message.value
            case format
            when :RAW
              STDOUT.write bytes
            when :GUESS
              value = guess_binary(bytes)
              value = pretty_binary(value.to_s)
              STDOUT.puts "#{head}\t#{m.offset}: #{value.to_s}"
            else
              STDOUT.puts "#{head}\t#{m.offset}: #{bytes.to_s}"
            end
          end
        else
          errmsg = Kafka::Protocol.errmsg(p.error_code)
          STDERR.puts "#{head}\t#{errmsg}".colorize(:red)
        end
      end
    end
  end

  protected def print_res(res : Kafka::Protocol::OffsetResponse)
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

  protected def print_count(ress : Array(Kafka::Protocol::OffsetResponse))
    records = ress.map{|res| extract_topic_counts(res)}.flatten
    counts  = records.group_by(&.topic).map{|topic, ary| TopicCount.new(topic, ary.sum(&.count))}
    # [Info::TopicCount(@topic="a", @count=2), Info::TopicCount(@topic="b", @count=0)]
    counts.sort_by(&.topic).each do |r|
      puts "#{r.count}\t#{r.topic}"
    end
  end

  protected def print_json(ress : Array(Kafka::Protocol::OffsetResponse))
    records = ress.map{|res| extract_topic_counts(res)}.flatten
    counts  = records.group_by(&.topic).map{|topic, ary| [topic, ary.sum(&.count)]}
    # [["a", 2], ["b", 0]]
    puts counts.to_h.to_json
  end

  protected def extract_topic_counts(res)
    res.topic_partition_offsets.map{ |meta|
      meta.partition_offsets.map{|po|
        unless po.error_code == 0
          errmsg = Kafka::Protocol.errmsg(po.error_code)
          STDERR.puts "#{meta.topic}##{po.partition}\t#{errmsg}"
        end
        TopicCount.new(meta.topic, po.count)
      }
    }
  end
end
