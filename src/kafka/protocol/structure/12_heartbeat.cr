######################################################################
### Heartbeat API (Key: 12):
### https://kafka.apache.org/protocol
module Kafka::Protocol::Structure
  structure HeartbeatRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String,
    group_id : String,
    generation_id : Int32,
    member_id : String

  structure HeartbeatResponseV0,
    correlation_id : Int32,
    error_code : Int16
end
