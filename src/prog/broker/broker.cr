require "../app"
require "json"

class Broker < App
  include Options
    
  options :broker, :count, :json, :verbose, :version, :help

  usage <<-EOF
Usage: kafka-broker [options]

Options:

Example:
  #{$0} -b localhost:9092
  #{$0} -j
EOF

  def execute
    if args.any?
      die "unknown args: #{args.inspect}"
    end

    topics = ["_"]              # dummy to avoid all topics
    req = Kafka::Protocol::MetadataRequest.new(0, "kafka-broker", topics)
    res = execute(req)

    if count && !json
      return print_res_as_count(res)
    end
    
    if json
      return print_res_as_json(res)
    end

    print_res(res)
  end

  private def print_res_as_count(res)
    puts res.brokers.size
  end
  
  private def print_res(res)
    res.brokers.sort(&.node_id).each do |b|
      puts "#{b.node_id}\t#{b.host}:#{b.port}"
    end
  end

  private def print_res_as_json(res)
    hash = {} of String => String | Int32
    res.brokers.sort(&.node_id).each do |b|
      hash[b.node_id.to_s] = "#{b.host}:#{b.port}"
    end
    hash["count"] = res.brokers.size if count
    puts hash.to_json
  end
end
