module Kafka::Protocol::Structure
  structure Partition,
    partition : Int32,
    time : Int64,
    max_number_of_offsets : Int32

  structure TopicAndPartitions,
    topic : String,
    partitions : Array(Partition)

  structure MetadataRequestMessage,
    topics : Array(String)

  structure MetadataRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    topics : Array(String)

  structure OffsetRequest,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    # message
    replica_id : Int32,
    topic_partitions : Array(TopicAndPartitions)

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
end
