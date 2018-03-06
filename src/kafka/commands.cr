require "./protocol"
require "./commands/*"

class Kafka
  module Commands
    include Kafka::Protocol::Utils

    var produce_opt : ProduceOption
    var fetch_opt : FetchOption
    
    ######################################################################
    ### 0: produce

    include Kafka::Commands::Produce

    alias SingleInsert = String | Binary
    alias MultiInsert = Array(String) | Array(Binary)
    alias Payload = SingleInsert | MultiInsert
    
    # Produces data into kafka
    #
    # Example:
    #
    # ```
    # kafka.produce("t1", "test")
    #
    # # insert multiple bodies at once
    # kafka.produce("t1", ["x", "y", "z"])
    # 
    # # available named options are `version`, `partition`
    # kafka.produce("t1", "test(v1)", version: 1)
    # kafka.produce("t1", "test(v1)", partition: 2)
    #
    # # binary data also can be given for payload
    # kafka.produce("t1", io.bytes)
    # ```
    def produce(topic : String, payload : Payload,
                version : Int32? = nil,
                partition : Int32? = nil,
                opt : ProduceOption = produce_opt.dup)
      opt.version = version.not_nil! if version
      opt.partition = partition.not_nil! if partition
      bodies = build_produce_bodies(payload)
      produce(Kafka::Entry.new(topic, opt.partition), bodies, opt)
    end

    private def build_produce_bodies(payload : Payload)
      case payload
      when SingleInsert ; [Kafka::Data.new(payload)]
      when MultiInsert  ; payload.map{|b| Kafka::Data.new(b)}
      else
        raise "[BUG] build_produce_bodies can't handle payload #{payload.class}"
      end
    end
    
    ######################################################################
    ### 1: fetch

    include Kafka::Commands::Fetch

    # Returns the message in the topic and partition and offset
    #
    # Example:
    #
    # ```
    # kafka.fetch("t1")       # => Kafka::Message("t1#0:0", "foo")
    # kafka.fetch("t1", 1)    # => Kafka::Message("t1#1:0", "bar")
    # kafka.fetch("t1", 0, 1) # => Kafka::Message("t1#0:1", "baz")
    # ```
    def fetch(topic : String,
              partition : Int32? = nil,
              offset : Int64? = nil,
              timeout : Time::Span? = nil,
              min_bytes : Int32? = nil,
              max_bytes : Int32? = nil,
              opt : FetchOption = fetch_opt.dup)
      opt.topic = topic
      opt.partition = partition.not_nil! if partition
      opt.offset = offset.not_nil! if offset
      opt.timeout = timeout.not_nil! if timeout
      opt.min_bytes = min_bytes.not_nil! if min_bytes
      opt.max_bytes = max_bytes.not_nil! if max_bytes
      fetch(opt)
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

    # Initialize producer id
    #
    # Example:
    #
    # ```
    # kafka.init_producer_id
    # ```
    include Kafka::Commands::InitProducerId
  end
end

