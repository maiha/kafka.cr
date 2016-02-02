require "./app"

class Fetch < App
  include Options
    
  options :broker, :topic, :offset, :max_bytes, :verbose, :help

  usage <<-EOF
Usage: kafka-fetch [options] [topics]

Options:

Example:
  ./bin/kafka-fetch topic1
  ./bin/kafka-fetch topic1 topic2
  ./bin/kafka-fetch -v topic1
EOF

  def do_list
    do_show([] of String)
  end

  def do_show(topics)
    replica = -1
    max_wait = 1000
    min_bytes = 0
    
    ps = Kafka::Protocol::Structure::FetchRequestPartitions.new(
      partition = 0, offset, max_bytes
    )
    ts = topics.map{|t|
      Kafka::Protocol::Structure::FetchRequestTopics.new(t, [ps])
    }

    req = Kafka::Protocol::FetchRequest.new(0, "kafka-fetch", replica, max_wait, min_bytes, ts)
    res = execute req

    print_res(res)
  end

  protected def execute(request)
    connect do |socket|
      bytes = request.to_slice
      spawn do
        socket.write bytes
        socket.flush
        sleep 0
      end

      recv = Kafka::Protocol.read(socket)

      if verbose
        STDERR.puts "recv: #{recv}"
        STDERR.flush
      end

      fake_io = MemoryIO.new(recv)
      return request.class.response.from_kafka(fake_io, verbose)
    end
  end

  private def print_res(res)
    res.topics.each do |t|
      t.partitions.each do |p|
        head = "#{t.topic}##{p.partition}"
        if p.error_code == 0
          p.message_sets.each do |m|
            STDOUT.puts "#{head}\t#{m.offset}: #{m.message.value}"
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
