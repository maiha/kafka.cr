module Kafka::Protocol::Structure
  ######################################################################
  ### Parts

  structure Broker,
    node_id : Int32,
    host : String,
    port : Int32 do

    def to_s
      "Broker(#{node_id}, #{host}:#{port})"
    end
  end

  structure Partition,
    partition : Int32,
    time : Int64,
    max_offsets : Int32 do

    def to_s
      "(#{partition}, #{time}, #{offsets})"
    end
  end

  structure TopicAndPartitions,
    topic : String,
    partitions : Array(Partition) do

    def to_s
      %(#{topic}(#{partitions.map(&.to_s).join(", ")}))
    end
  end

  structure PartitionMetadata,
    error_code : Int16,
    id : Int32,
    leader : Int32,
    replicas : Array(Int32),
    isrs : Array(Int32) do

    def to_s
      msg = errmsg(error_code, "{leader=#{leader},replica=#{replicas.inspect},isr=#{isrs.inspect}}")
      %(#{id} => #{msg})
    end
  end

  structure TopicMetadata,
    error_code : Int16,
    name : String,
    partitions : Array(PartitionMetadata) do

    def to_s
      %(#{name}(#{errmsg(error_code, partitions.map(&.to_s).join(", "))}))
    end
  end

  structure PartitionOffset,
    partition : Int32,
    error_code : Int16,
    offsets : Array(Int64)

  structure TopicPartitionOffset,
    topic : String,
    partition_offsets : Array(PartitionOffset)

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
