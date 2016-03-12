module Kafka::Protocol::Structure
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
    getter size, message_sets
    def initialize(@size : Int32, @message_sets : Array(MessageSet))
    end

    def self.new(message_sets : Array(MessageSet))
      new(message_sets.size, message_sets)
    end

    def to_kafka(io : IO)
      size.to_kafka(io)
      message_sets.each do |set|
        set.to_kafka(io)
      end
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
    topics : Array(TopicMetadata)

    structure TopicMetadata,
      error_code : Int16,
      name : String,
      partitions : Array(PartitionMetadata)

  structure ProduceRequest,
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

  structure ProduceResponse,
    correlation_id : Int32

  structure OffsetRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    topic_partitions : Array(TopicAndPartitions)

  structure OffsetResponse,
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
end

require "./structure/*"
