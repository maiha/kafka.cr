module Kafka::Protocol::Structure
  module Record
    abstract def offset : Int64
    abstract def key : Bytes
    abstract def value : Bytes
  end
  
  structure RecordImpl,
    length : Varint,
    attributes : Int8,
    timestamp_delta : Varlong,
    offset_delta : Varint,
    key : Varbytes,
    val : Varbytes,
    headers : VarArray(Header)
end
