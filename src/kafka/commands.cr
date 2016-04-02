require "./protocol"
require "./commands/*"

class Kafka
  module Commands
    include Kafka::Protocol::Utils

    ######################################################################
    ### 0: produce

    include Kafka::Commands::Produce

    alias Payload = String | Binary
    
    # Produces data into kafka by v1
    #
    # Example:
    #
    # ```
    # kafka.produce_v1("t1", 0, "test")
    # ```
    def produce_v0(topic : String, partition : Int32, body : Payload)
      produce_v0(topic, partition, [body])
    end

    # bulk insert
    def produce_v0(topic : String, partition : Int32, bodies : Array(Payload))
      produce_v0(Kafka::Entry.new(topic, partition), bodies.map{|b|Kafka::Data.new(b)}, ProduceOption.default)
    end

    def produce_v1(topic : String, partition : Int32, body : Payload)
      produce_v1(topic, partition, [body])
    end

    # bulk insert
    def produce_v1(topic : String, partition : Int32, bodies : Array(Payload))
      produce_v1(Kafka::Entry.new(topic, partition), bodies.map{|b|Kafka::Data.new(b)}, ProduceOption.default)
    end
    
    ######################################################################
    ### 1: fetch

    include Kafka::Commands::Fetch

    # Returns the message in the topic and partition and offset
    #
    # Example:
    #
    # ```
    # kafka.fetch("t1", 0, 0_i64) # => Kafka::Message("t1[0]#0", "test")
    # ```
    def fetch(topic : String, partition : Int32, offset : Int64, timeout : Time::Span = 1.second, min_bytes : Int32 = 0, max_bytes : Int32 = 1024)
      idx = Kafka::Index.new(topic, partition, offset)
      opt = FetchOption.new(timeout, min_bytes, max_bytes)
      fetch(idx, opt)
    end

    ######################################################################
    ### 3: metadata

    include Kafka::Commands::Metadata

    # Returns the metadata information
    #
    # Example:
    #
    # ```
    # kafka.metadata # => [Kafka::MetadataInfo, ...]
    # kafka.metadata.brokers # => [#<Kafka::Broker @host="localhost", @port=9092>]
    # kafka.metadata.topics # => [Kafka::TopicInfo(@name="t1", @partition=0, @leader=1, @replicas=[1], @isrs=[1])]
    # ```
    # - NOTE: topics returns one of Kafka::TopicInfo and Kafka::TopicError
    def metadata(topics : Array(String))
      metadata(topics, MetadataOption.zero)
    end

    # as same as `metadata` except it returns raw response object
    #
    # Example:
    #
    # ```
    # kafka.raw_metadata # => #<Kafka::Protocol::MetadataResponse:0x1651180 @brokers=... @topics...>
    # ```
    def raw_metadata(topics : Array(String))
      raw_metadata(topics, MetadataOption.zero)
    end

    ######################################################################
    ### topic

    include Kafka::Commands::Topics

    # Returns the topic information
    #
    # Example:
    #
    # ```
    # kafka.topics # => [Kafka::TopicInfo, ...]
    # ```
    def topics(names : Array(String) = [] of String, consumer_offsets : Bool = false) : Array(Kafka::TopicInfo)
      opt = TopicsOption.new(consumer_offsets)
      topics(names, opt)
    end

    # Returns the topic information
    #
    # Example:
    #
    # ```
    # kafka.create_topic("t1", 1, 1)
    # ```
    def create_topic(name : String, partition : Int32, replication : Int32)
      raise "not implemented yet"
    end

    ######################################################################
    ### offset

    include Kafka::Commands::Offset

    # Returns the offset information of the topic and partition
    #
    # Example:
    #
    # ```
    # kafka.offset("t1", 0) # => Kafka::Offset("t1[0]", count=102, offsets=[102, 0])
    # ```
    def offset(topic : String, partition : Int32)
      idx = Kafka::Index.new(topic, partition, -1_i64)
      opt = OffsetOption.new(-1_i64, 999999999)
      offset(idx, opt)
    end

    ######################################################################
    ### general

    def execute(request : Kafka::Request, handler : Kafka::Handler)
      Kafka::Execution.execute(connection, request, handler)
    end

    def execute(request : Kafka::Request)
      execute(request, handler)
    end
  end
end

