######################################################################
### Metadata API (Key: 3):
### https://kafka.apache.org/protocol#
module Kafka::Protocol::Structure
  structure TopicMetadata,
    error_code : Int16,
    name : String,
    partitions : Array(PartitionMetadata)

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

  ######################################################################
  ### V5

  structure BrokerV5,
    node_id : Int32,
    host : String,
    port : Int32,
    rack : String

  structure TopicMetadataV5,
    error_code : Int16,
    name : String,
    is_internal : Bool,
    partitions : Array(PartitionMetadataV5)

  structure PartitionMetadataV5,
    error_code : Int16,
    id : Int32,
    leader : Int32,
    replicas : Array(Int32),
    isrs : Array(Int32),
    offline_replicas : Array(Int32)
  
  structure MetadataRequestV5,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    topics : Array(String),
    allow_auto_topic_creation : Bool

  structure MetadataResponseV5,
    correlation_id : Int32,
    throttle_time_ms : Int32,
    brokers : Array(BrokerV5),
    cluster_id : String,
    controller_id : Int32,
    topics : Array(TopicMetadataV5) do

    def to_s(io : IO)
      io << "brokers: %s\n" % brokers.map(&.to_s).join(", ")
      io << "topics: %s\n" % topics.map(&.to_s).join(", ")
    end
  end
end
