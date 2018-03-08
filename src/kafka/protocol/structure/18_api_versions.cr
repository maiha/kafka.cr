######################################################################
### ApiVersions API (Key: 18):
### https://kafka.apache.org/protocol#The_Messages_InitProducerId
module Kafka::Protocol::Structure
  structure ApiVersion,
    api_key : Int16,
    min_version : Int16,
    max_version : Int16

  ######################################################################
  ### V0
  structure ApiVersionsRequestV0,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String

  structure ApiVersionsResponseV0,
    error_code : Int16,
    api_versions : Array(ApiVersion)

  ######################################################################
  ### V1
  structure ApiVersionsRequestV1,
    api_key : Int16,
    api_version : Int16,
    correlation_id : Int32,
    client_id : String

  structure ApiVersionsResponseV1,
    error_code : Int16,
    unknown : Int32,
    api_versions : Array(ApiVersion),
    throttle_time_ms : Int32
end
