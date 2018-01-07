require "../app"

class Metadata < App
  include Options

  options :all, :broker, :consumer_offsets, :dump, :topic, :nop, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] [topics]

Options:

Example:
  #{PROGRAM_NAME} topic1 topic2
  #{PROGRAM_NAME} -a
  #{PROGRAM_NAME} -b localhost:9092 -a
  #{PROGRAM_NAME} -v topic1
EOF

  def do_show(req)
    res = execute(req)
    print_res(res)
  end

  def do_nop(req)
    bytes = req.to_slice

    if dump
      p bytes
    else
      p req
    end
  end

  def do_topics(topics)
    req = Kafka::Protocol::MetadataRequest.new(0, "kafka-metadata", topics)
    if nop
      do_nop(req)
    else
      do_show(req)
    end
  end
  
  def execute
    topics = args.reject(&.empty?)

    if topics.any? || all
      do_topics(topics)
    else
      die "no topics. specify topic name, or use `-a' to show all topics"
    end
  end

  private def print_res(res)
    # show brokers
    res.brokers.each do |b|
      puts "broker##{b.node_id}\t#{b.host}:#{b.port}"
    end

    # show topics
    res.topics.each do |t|
      next if !consumer_offsets && t.name == "__consumer_offsets"
      if t.error_code == -1
        errmsg = Kafka::Protocol.errmsg(t.error_code)
        puts "#{t.name}\t#{errmsg}".colorize(:red)
      else
        t.partitions.each do |p|
          if p.error_code == -1
            errmsg = Kafka::Protocol.errmsg(p.error_code)
            STDERR.puts "#{t.name}##{p.id}\t#{errmsg}".colorize(:red)
            STDERR.flush
          else
            puts "#{t.name}##{p.id}\tleader=#{p.leader},replica=#{p.replicas.inspect},isr=#{p.isrs.inspect}"
          end
        end
      end
    end          
  end
end
