require "./records/*"

module Kafka::Protocol::Structure
  include ZigZag
  include Records

  # NOTE: Kafka has two nullable types for `String` and `Bytes`.
  # 1. But, Crystal can't differ nils whether it is `String?` or `Bytes?`.
  # 2. Fortunately, both `NullableString` and emtpy string builds `short int(-1)`.
  # So, we use `String` rather than `String?` for `transactional_id`.

  ######################################################################
  ### Parts

  alias Bytes = Slice(UInt8)

  Null = Slice(UInt8).new(0)

  def null
    Null
  end

  class VarArray(T) < Array(T)
  end

  structure Header,
    key : String,
    value : Bytes

  structure Message,
    crc : Int32,
    magic_byte : Int8,
    attributes : Int8,
    key : Bytes,
    value : Bytes

  structure MessageSet,
    offset : Int64,
    bytesize : Int32,
    message : Message

  record Varbytes, size : Int32, bytes : Bytes

  class MessageSetEntry
    SIZE_NOT_CALCULATED = -1

    getter size, message_sets
    # NOTE: size is mutable for performance reasons

    def initialize(@size : Int32, @message_sets : Array(MessageSet))
    end

    def initialize(message_sets : Array(MessageSet))
      initialize(SIZE_NOT_CALCULATED, message_sets)
    end

    def to_kafka(io : IO)
      case size
      when SIZE_NOT_CALCULATED
        to_kafka_with_calculation(io)
      else
        size.to_kafka(io)
        message_sets.each do |set|
          set.to_kafka(io)
        end
      end
    end

    private def to_kafka_with_calculation(io : IO)
      fake = IO::Memory.new
      message_sets.each do |set|
        set.to_kafka(fake)
      end

      @size = fake.bytesize
      @size.to_kafka(io)
      io.write(fake.to_slice)
    end    
  end
  
  structure MessageV1,
    crc : Int32,
    magic_byte : Int8,
    attributes : Int8,
    timestamp : Int64,
    key : Bytes,
    value : Bytes

  # clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java
  structure RecordBatchV2,
    base_offset : Int64,
    length : Int32,
    partition_leader_epoch : Int32,
    magic : Int8,
    crc : UInt32,
    attributes : Int16,
    last_offset_delta : Int32,
    first_timestamp : Int64,
    max_timestamp : Int64,
    producer_id : Int64,
    producer_epoch : Int16,
    base_sequence : Int32,
    records : Array(Record)

  structure Broker,
    node_id : Int32,
    host : String,
    port : Int32

  structure Partition,
    partition : Int32,
    time : Int64,
    max_offsets : Int32

  structure TopicAndPartitions,
    topic : String,
    partitions : Array(Partition)
end

require "./structure/*"
