class Kafka::Broker
  getter! host
  getter! port

  DEFAULT_HOST = "127.0.0.1"
  DEFAULT_PORT = 9092

  def initialize(@host : String, @port = DEFAULT_PORT : Int32)
  end

  def self.default
    Kafka::Broker.new(DEFAULT_HOST, DEFAULT_PORT)
  end

  def self.parse(str : String) : Broker
    case str
    when /\A:([0-9]+)\Z/
      new(DEFAULT_HOST, $1.to_i)
    when /\A[a-zA-Z0-9_\-\.]+\Z/
      new(str)
    when /\A([a-zA-Z0-9_\-\.]+):(\d+)\Z/
      new($1, $2.to_i)
    else
      raise "Unable to parse #{str} to a broker"
    end
  end
end
