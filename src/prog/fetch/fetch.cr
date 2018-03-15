require "../app"

class Fetch < App
  include Options
  include Kafka::Protocol
  
  option max_wait_time : Int32, "--max-wait-time MSEC", "Maximum time in ms to wait for the response.", 1000
  option min_bytes : Int32, "--min-bytes SIZE", "Minimum bytes to accumulate in the response", 0
  option max_bytes : Int32, "--max-bytes SIZE", "Maximum bytes to accumulate in the response", 1048576
  option isolation_level : Int32, "--isolation_level LEVEL", "This setting controls the visibility of transactional records.", 0
  option log_start_offset : Int64, "--log_start_offset VALUE", "Earliest available offset of the follower replica.", 0_i64
  option last : Bool, "-l", "--last", "Show last message", false
  option strict : Bool, "--strict", "Do not discover leader broker automatically", false
  options :api_ver, :correlation_id, :broker, :partition, :topic, :offset, :max_bytes, :raw, :guess, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topic]

Options:

Example:
  #{PROGRAM_NAME} topic1
  #{PROGRAM_NAME} topic1 -l      # fetch only last msg
  #{PROGRAM_NAME} topic1 -g      # print guessed payload type
  #{PROGRAM_NAME} topic1 --ver 6 # specify api version
EOF

  def execute
    topics = ([topic] + args).reject(&.empty?).uniq

    if topic = topics.first?
      if last
        do_last(topic)
      else
        do_show(topic, offset)
      end
    else
      die "please specify one topic"
    end
  end

  def do_show(topic, offset)
    case api_ver
    when 0, -1
      req = build_fetch_request_v0(topic, offset)
      res = execute(req)
      res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
      print_res(res, format)
    when 6
      req = build_fetch_request_v6(topic, offset, log_start_offset)
      res = execute(req)
      res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
      print_res(res, format)
    else
      raise "Fetch Request supports only 0 or 6 for api version (given #{api_ver})"
    end
  end
 
  def do_last(topic)
    req = build_offset_request([topic], partition)
    res = execute(req)
    res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res) && !strict
    offset = extract_last_offset!(res, topic)
    do_show(topic, offset)
  end

  protected def execute(request, broker = build_broker)
    Kafka.debug = true if verbose
    kafka = Kafka.new(broker)

    handler = Kafka::Handlers::Config.new
    handler.recv = ->(bytes: Slice(UInt8)) {
      puts "DEBUG: got #{bytes.size} bytes"; nil
    } if verbose

    kafka.execute(request, handler)
  end

  private def not_leader?(res : FetchResponseV0)
    res.topics.each do |t|
      t.partitions.each do |p|
        return true if p.error_code == 6
      end
    end
    return false
  end
  
  private def not_leader?(res : FetchResponseV6)
    res.responses.any?{|r|
      r.partition_responses.any?{|p|
        p.partition_header.error_code == Errors::NotLeaderForPartitionCode.value
      }
    }
  end
  
  protected def print_res(res : FetchResponseV6, format)
    res.responses.each do |r|
      r.partition_responses.each do |pr|
        p = pr.partition_header
        head = "#{r.topic}##{p.partition}"
        if p.error_code == 0
          pr.record_set.records.each do |record|
            offset = pr.record_set.base_offset + record.offset_delta.value
            bytes = record.val.bytes
            case format
            when :RAW
              STDOUT.write bytes
            when :GUESS
              value = guess_binary(bytes)
              value = pretty_binary(value.to_s)
              puts "#{head}\t#{offset}: #{value.to_s}"
            else
              puts "#{head}\t#{offset}: #{bytes.to_s}"
            end
          end
        else
          errmsg = Kafka::Protocol.errmsg(p.error_code)
          logger.error "#{head}\t#{errmsg}".colorize(:red)
        end
      end
    end
  end

  private def format
    return :RAW if raw
    return :GUESS if guess
    return true
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
  
  private def build_fetch_request_v0(topic, offset : Int64)
    replica = -1
    ps = [Kafka::Protocol::Structure::FetchRequestV0::Partition.new(partition, offset, max_bytes)]
    ts = [Kafka::Protocol::Structure::FetchRequestV0::Topic.new(topic, ps)]

    return Kafka::Protocol::FetchRequestV0.new(0, app_name, replica, max_wait_time, min_bytes, ts)
  end

  private def build_fetch_request_v6(topic, fetch_offset, log_start_offset)
    replica_id = -1
    partitions = [Kafka::Protocol::Structure::FetchRequestV6::Partitions.new(partition, fetch_offset, log_start_offset, max_bytes)]
    topics = [Kafka::Protocol::Structure::FetchRequestV6::TopicPartitions.new(topic, partitions)]
    return Kafka::Protocol::FetchRequestV6.new(correlation_id, app_name, replica_id, max_wait_time, min_bytes, max_bytes, isolation_level.to_i8, topics)
  end
end
