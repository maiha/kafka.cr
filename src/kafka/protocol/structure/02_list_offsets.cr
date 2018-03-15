######################################################################
### ListOffsets API (Key: 2):
### https://kafka.apache.org/protocol
module Kafka::Protocol::Structure
  structure ListOffsetsRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    topic_partitions : Array(TopicAndPartitions)

  structure ListOffsetsResponseV0,
    correlation_id : Int32,
    topic_partition_offsets : Array(TopicPartitionOffset) do

    structure TopicPartitionOffset,
      topic : String,
      partition_offsets : Array(PartitionOffset)

    structure PartitionOffset,
      partition : Int32,
      error_code : Int16,
      offsets : Array(Int64) do

      include Kafka::OffsetsReader
    end
  end
end
