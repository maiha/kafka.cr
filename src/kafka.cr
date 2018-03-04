require "logger"

require "crc32"
require "msgpack"

class Kafka
end

require "./predef/*"
require "./utils/*"

require "./kafka/data"
require "./kafka/*"
require "./prog/*"

class Kafka
  @broker     : Kafka::Broker
  @connection : Kafka::Connection
  @client_id  : String
  @handler    : Kafka::Handlers::Config

  private getter :broker

  getter :connection
  delegate socket!, close, connected?, to: connection

  property! :client_id
  property! :handler

  include Kafka::Commands

  ######################################################################
  ### printings

  def inspect(io : IO)
    if connected?
      io << "Kafka('#{broker.host}:#{broker.port}'[connected])"
    else
      io << "Kafka('#{broker.host}:#{broker.port}'[closed])"
    end
  end

  ######################################################################
  ### instance creations

  def self.open(broker : String = "localhost")
    kafka = Kafka.new(Broker.parse(broker))
    begin
      yield(kafka)
    ensure
      kafka.close
    end
  end

  def initialize(broker : String)
    initialize(Kafka::Broker.parse(broker))
  end

  def initialize(host : String, port : Int32)
    initialize(Broker.new(host, port))
  end

  def initialize(@broker : Broker = Broker.default)
    @connection = Kafka::Connection.new(broker.host, broker.port)
    @client_id = "kafka.cr"
    @handler = Kafka::Handlers::Config.new
  end
end
