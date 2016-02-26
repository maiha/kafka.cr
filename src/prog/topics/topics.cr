require "../app"

class Topics < App
  include Options

#  option create : Bool, "-c", "--create", "Create a new topic", false
  # parser.on("-p NUM", "--partitions=NUM", "The number of partitions for the topic") { |p| @partitions = p }
  # parser.on("-r NUM", "--replication-factor=NUM", "The number of replication factor for the topic") { |r| @replication_factor = r }
  option simple : Bool, "-s", "--simple", "Show topic names only", false
    
  options :all, :broker, :consumer_offsets, :verbose, :version, :help
  
  usage <<-EOF
Usage: #{app_name} [options] [topics]

Options:

Example:
  #{$0} -b :9092 --all
  #{$0} topic1
  #{$0} topic1 topic2
  #{$0} -v topic1 
EOF
#  [not implemented] #{$0} --create --partitions=1 --replication-factor=1 --topic=topic1

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

  def execute
    if all
      return do_list
    end

    topics = args.reject(&.empty?)

#    if create
#      case topics.size
#      when 1 ; return do_create(topics.first.not_nil!)
#      when 0 ; die "no topics. `create' needs one topic"
#      else   ; die "too many topics. `create' needs one topic"
#      end
#    end

    if topics.any?
      return do_show(topics)
    end

    die "no topics"

  rescue err
    die err
  end

  private def print_res(res)
    res.topics.each do |meta|
      if meta.error_code == 0
        if !consumer_offsets && meta.name == "__consumer_offsets"
          # skip
        elsif simple
          STDOUT.puts meta.name
        else
          meta.partitions.each do |pm|
            if pm.error_code == 0
              STDOUT.puts "#{meta.name}##{pm.id}\tleader=#{pm.leader}, replicas=#{pm.replicas.inspect}, isrs=#{pm.isrs.inspect}"
            else
              err = Kafka::Protocol.errmsg(pm.error_code)
              STDOUT.puts "#{meta.name}##{pm.id}\t#{err}"
            end
          end
        end
      else
        STDERR.puts "ERROR: #{meta.to_s}"
      end
    end
  end
end
