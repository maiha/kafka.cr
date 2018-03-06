module Kafka::Protocol::Structure
  # NOTE: Kafka has two nullable types for `String` and `Bytes`.
  # 1. But, Crystal can't differ nils whether it is `String?` or `Bytes?`.
  # 2. Fortunately, both `NullableString` and emtpy string builds `short int(-1)`.
  # So, we use `String` rather than `String?` for `transactional_id`.

  ######################################################################
  ### Parts

  alias Bytes = Slice(UInt8)

  structure Message,
    crc : Int32,
    magic_byte : Int8,
    attributes : Int8,
    key : Bytes,
    value : Bytes

  structure MessageSet,
    offset : Int64,
    bytesize : Int32,
    message : Message

  class MessageSetEntry
    SIZE_NOT_CALCULATED = -1

    getter size, message_sets
    # NOTE: size is mutable for performance reasons

    def initialize(@size : Int32, @message_sets : Array(MessageSet))
    end

    def initialize(message_sets : Array(MessageSet))
      initialize(SIZE_NOT_CALCULATED, message_sets)
    end

    def to_kafka(io : IO)
      case size
      when SIZE_NOT_CALCULATED
        to_kafka_with_calculation(io)
      else
        size.to_kafka(io)
        message_sets.each do |set|
          set.to_kafka(io)
        end
      end
    end

    private def to_kafka_with_calculation(io : IO)
      fake = IO::Memory.new
      message_sets.each do |set|
        set.to_kafka(fake)
      end

      @size = fake.bytesize
      @size.to_kafka(io)
      io.write(fake.to_slice)
    end    
  end
  
  structure Broker,
    node_id : Int32,
    host : String,
    port : Int32

  structure Partition,
    partition : Int32,
    time : Int64,
    max_offsets : Int32

  structure TopicAndPartitions,
    topic : String,
    partitions : Array(Partition)

  structure PartitionMetadata,
    error_code : Int16,
    id : Int32,
    leader : Int32,
    replicas : Array(Int32),
    isrs : Array(Int32)

  ######################################################################
  ### Request and Response

  structure MetadataRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    topics : Array(String)

  structure MetadataResponse,
    correlation_id : Int32,
    brokers : Array(Broker),
    topics : Array(TopicMetadata) do

    def to_s(io : IO)
      io << "brokers: %s\n" % brokers.map(&.to_s).join(", ")
      io << "topics: %s\n" % topics.map(&.to_s).join(", ")
    end
  end

    structure TopicMetadata,
      error_code : Int16,
      name : String,
      partitions : Array(PartitionMetadata)

  structure ProduceRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    required_acks : Int16,
    timeout : Int32,
    topic_partitions : Array(TopicAndPartitionMessages)

    structure TopicAndPartitionMessages,
      topic : String,
      partition_messages : Array(PartitionMessage)

      structure PartitionMessage,
        partition : Int32,
        message_set_entry : MessageSetEntry

  structure ProduceResponseV0,
    correlation_id : Int32,
    topics : Array(TopicProducedV0)

    structure TopicProducedV0,
      topic : String,
      partitions : Array(PartitionProducedV0)

      structure PartitionProducedV0,
        partition : Int32,
        error_code : Int16,
        offset : Int64

  # same as ProduceRequestV0
  structure ProduceRequestV1,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    required_acks : Int16,
    timeout : Int32,
    topic_partitions : Array(TopicAndPartitionMessages)

  structure ProduceResponseV1,
    correlation_id : Int32,
    topics : Array(TopicProducedV0),
    throttle_time : Int32
  
  structure ProduceRequestV3,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    transactional_id : String,
    required_acks : Int16,
    timeout : Int32,
    topic_partitions : Array(TopicAndPartitionMessages)

  structure ProduceResponseV3,
    correlation_id : Int32,
    topics : Array(TopicProducedV0),
    throttle_time : Int32
  
  structure ListOffsetsRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    topic_partitions : Array(TopicAndPartitions)

  structure ListOffsetsResponse,
    correlation_id : Int32,
    topic_partition_offsets : Array(TopicPartitionOffset)

    structure TopicPartitionOffset,
      topic : String,
      partition_offsets : Array(PartitionOffset)

      structure PartitionOffset,
        partition : Int32,
        error_code : Int16,
        offsets : Array(Int64)

  structure HeartbeatRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    group_id : String,
    generation_id : Int32,
    member_id : String

  structure HeartbeatResponse,
    correlation_id : Int32,
    error_code : Int16

  structure FetchRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    max_wait_time : Int32,
    min_bytes : Int32,
    topics : Array(FetchRequestTopics)

    structure FetchRequestTopics,
      topic : String,
      partitions : Array(FetchRequestPartitions)

      structure FetchRequestPartitions,
        partition : Int32,
        offset : Int64,
        max_bytes : Int32
  
  structure FetchResponse,
    correlation_id : Int32,
    topics : Array(FetchResponseTopic)

    structure FetchResponseTopic,
      topic : String,
      partitions : Array(FetchResponsePartition)

      structure FetchResponsePartition,
        partition : Int32,
        error_code : Int16,
        high_water_mark : Int64,
        message_set_entry : MessageSetEntry

  ######################################################################
  ### InitProducerId API (Key: 22):
  ### https://kafka.apache.org/protocol#The_Messages_InitProducerId
  structure InitProducerIdRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    transactional_id : String,
    transaction_timeout_ms : Int32

  structure InitProducerIdResponse,
    throttle_time_ms : Int32,
    error_code : Int16,
    producer_id : Int64,
    producer_epoch : Int16
end

require "./structure/*"
