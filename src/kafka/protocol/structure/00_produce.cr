######################################################################
### InitProducerId API (Key: 22):
### https://kafka.apache.org/protocol#The_Messages_InitProducerId
module Kafka::Protocol::Structure
  structure TopicAndPartitionMessages,
    topic : String,
    partition_messages : Array(PartitionMessage)

  structure PartitionMessage,
    partition : Int32,
    message_set_entry : MessageSetEntry

  ######################################################################
  ### V0
  structure ProduceRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    required_acks : Int16,
    timeout : Int32,
    topic_partitions : Array(TopicAndPartitionMessages)

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

  ######################################################################
  ### V5 : Request
  structure ProduceRequestV5,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    transactional_id : String,
    acks : Int16,
    timeout : Int32,
    topic_data : Array(TopicData) do

    structure TopicData,
      topic : String,
      data : Array(PartitionRecordSet)

    structure PartitionRecordSet,
      partition : Int32,
      start_offset : Int32, # length_field?
      record_set : MemoryRecords
  end
  
  ######################################################################
  ### V5 : Response
  structure ProduceResponseV5,
    correlation_id : Int32,
    responses : Array(TopicPartitionResponse),
    throttle_time_ms : Int32 do

    structure TopicPartitionResponse,
      topic : String,
      partition_responses : Array(PartitionResponse)

    structure PartitionResponse,
      partition : Int32,
      error_code : Int16,
      base_offset : Int64,
      log_append_time : Int64,
      log_start_offset : Int64
  end  
end
