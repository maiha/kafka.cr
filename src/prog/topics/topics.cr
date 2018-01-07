require "../app"

class Topics < App
  include Options

#  option create : Bool, "-c", "--create", "Create a new topic", false
  # parser.on("-p NUM", "--partitions=NUM", "The number of partitions for the topic") { |p| @partitions = p }
  # parser.on("-r NUM", "--replication-factor=NUM", "The number of replication factor for the topic") { |r| @replication_factor = r }
  option simple : Bool, "-s", "--simple", "Show topic names only", false
  option count : Bool, "-c", "--count", "Just count simply", false
  option before : Int64, "--before MSEC", "Used to ask for all messages before a certain time (ms)", -1
    
  option_all true
  options :broker, :consumer_offsets, :verbose, :version, :help
  
  usage <<-EOF
Usage: #{app_name} [options] [topics]

Options:

Example:
  #{PROGRAM_NAME} -b :9092 --all
  #{PROGRAM_NAME} topic1
  #{PROGRAM_NAME} topic1 topic2
  #{PROGRAM_NAME} -v topic1 
EOF
#  [not implemented] #{PROGRAM_NAME} --create --partitions=1 --replication-factor=1 --topic=topic1

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    req = Kafka::Protocol::MetadataRequest.new(0, "kafka-topics", topics)
    res = execute req
    print_res(res)
  end

  def do_create(topic : String)
    die "Sorry, `create' is not implemented yet"
  end

  def do_count(topics)
    meta = fetch_topic_metadata(topics, app_name)
    ress = fetch_offsets(meta, before.to_i64)
    print_count(ress)
  end
  
  def execute
    topics = args.reject(&.empty?)
    self.all = false if topics.any?

    if all
      if count
        return do_count(topics)
      else
        return do_list
      end
    end

#    if create
#      case topics.size
#      when 1 ; return do_create(topics.first.not_nil!)
#      when 0 ; die "no topics. `create' needs one topic"
#      else   ; die "too many topics. `create' needs one topic"
#      end
#    end

    die "no topics" unless topics.any?

    if count
      return do_count(topics)
    else
      return do_show(topics)
    end

  rescue err
    die err
  end

  private def print_res(res)
    res.topics.each do |meta|
      if meta.error_code == 0
        if !consumer_offsets && meta.name == "__consumer_offsets"
          # skip
        elsif simple
          logger.info meta.name
        else
          meta.partitions.each do |pm|
            if pm.error_code == 0
              logger.info "#{meta.name}##{pm.id}\tleader=#{pm.leader}, replicas=#{pm.replicas.inspect}, isrs=#{pm.isrs.inspect}"
            else
              err = Kafka::Protocol.errmsg(pm.error_code)
              logger.info "#{meta.name}##{pm.id}\t#{err}"
            end
          end
        end
      else
        logger.error "ERROR: #{meta.to_s}"
      end
    end
  end
end
