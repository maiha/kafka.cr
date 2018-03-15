######################################################################
### InitProducerId API (Key: 22):
### https://kafka.apache.org/protocol#The_Messages_InitProducerId
module Kafka::Protocol::Structure
  structure InitProducerIdRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    transactional_id : String,
    transaction_timeout_ms : Int32

  structure InitProducerIdResponseV0,
    correlation_id : Int32,
    throttle_time_ms : Int32,
    error_code : Int16,
    producer_id : Int64,
    producer_epoch : Int16
end
