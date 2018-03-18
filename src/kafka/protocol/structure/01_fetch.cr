######################################################################
### Fetch API (Key: 1):
### https://kafka.apache.org/protocol
module Kafka::Protocol::Structure
  ######################################################################
  ### V0
  structure FetchRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    max_wait_time : Int32,
    min_bytes : Int32,
    topics : Array(Topic) do

    structure Partition,
      partition : Int32,
      offset : Int64,
      max_bytes : Int32

    structure Topic,
      topic : String,
      partitions : Array(FetchRequestV0::Partition)
  end

  structure FetchResponseV0,
    correlation_id : Int32,
    topics : Array(Topic) do

    structure Topic,
      topic : String,
      partitions : Array(FetchResponseV0::Partition)

    structure Partition,
      partition : Int32,
      error_code : Int16,
      high_water_mark : Int64,
      message_set_entry : MessageSetEntry do

      delegate message_sets, to: message_set_entry
    end
  end

  ######################################################################
  ### V6 : Request
  structure FetchRequestV6,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    replica_id : Int32,
    max_wait_time : Int32,
    min_bytes : Int32,
    max_bytes : Int32,
    isolation_level : Int8,
    topics : Array(TopicPartitions) do

    structure TopicPartitions,
      topic : String,
      partitions : Array(Partitions)

    structure Partitions,
      partition : Int32,
      fetch_offset : Int64,
      log_start_offset : Int64,
      max_bytes : Int32
  end

  ######################################################################
  ### V6 : Response
  structure FetchResponseV6,
    correlation_id : Int32,
    throttle_time_ms : Int32,
    topics : Array(FetchResponseV6::Topic) do

    structure Topic,
      topic : String,
      partitions : Array(FetchResponseV6::Partition)

    structure Partition,
      partition_header : PartitionHeader,
      record_set : MemoryRecords

    structure PartitionHeader,
      partition : Int32,
      error_code : Int16,
      high_water_mark : Int64,
      last_stable_offset : Int64,
      log_start_offset : Int64,
      aborted_transactions : Array(AbortedTransaction)

    structure AbortedTransaction,
      producer_id : Int64,
      first_offset : Int64

    # eagar loading
    def load!
      topics.each do |topic|
        topic.partitions.each do |partition|
          partition.record_set.each do |record_batch|
            record_batch.each do |record|
              # record.to_s
            end
          end
        end
      end
    end
  end
end
