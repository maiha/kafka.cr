class Kafka::Cluster::Broker
  getter! host
  getter! port

  DEFAULT_PORT = 9092

  def initialize(@host : String, @port = DEFAULT_PORT : Int32)
  end
    
  def self.parse(str : String) : Broker
    case str
    when /\A[a-zA-Z0-9_\.]+\Z/
      new(str)
    when /\A([a-zA-Z0-9_\.]+):(\d+)\Z/
      new($1, $2.to_i)
    else
      raise "Unable to parse #{str} to a broker"
    end
  end
end
