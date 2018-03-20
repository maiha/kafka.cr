require "./record_batch"

module Kafka::Protocol::Structure
  class DefaultRecordBatch
    include RecordBatch

    BASE_OFFSET_OFFSET            = 0
    BASE_OFFSET_LENGTH            = 8
    LENGTH_OFFSET                 = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH
    LENGTH_LENGTH                 = 4
    PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH
    PARTITION_LEADER_EPOCH_LENGTH = 4
    MAGIC_OFFSET                  = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH
    MAGIC_LENGTH                  = 1
    CRC_OFFSET                    = MAGIC_OFFSET + MAGIC_LENGTH
    CRC_LENGTH                    = 4
    ATTRIBUTES_OFFSET             = CRC_OFFSET + CRC_LENGTH
    ATTRIBUTE_LENGTH              = 2
    LAST_OFFSET_DELTA_OFFSET      = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH
    LAST_OFFSET_DELTA_LENGTH      = 4
    FIRST_TIMESTAMP_OFFSET        = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH
    FIRST_TIMESTAMP_LENGTH        = 8
    MAX_TIMESTAMP_OFFSET          = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH
    MAX_TIMESTAMP_LENGTH          = 8
    PRODUCER_ID_OFFSET            = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH
    PRODUCER_ID_LENGTH            = 8
    PRODUCER_EPOCH_OFFSET         = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH
    PRODUCER_EPOCH_LENGTH         = 2
    BASE_SEQUENCE_OFFSET          = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH
    BASE_SEQUENCE_LENGTH          = 4
    RECORDS_COUNT_OFFSET          = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH
    RECORDS_COUNT_LENGTH          = 4
    RECORDS_OFFSET                = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH
    RECORD_BATCH_OVERHEAD         = RECORDS_OFFSET

    COMPRESSION_CODEC_MASK  = 0x07
    TRANSACTIONAL_FLAG_MASK = 0x10
    CONTROL_FLAG_MASK       = 0x20
    TIMESTAMP_TYPE_MASK     = 0x08

    property count : Int32
    property attributes : Int8    
    property magic : Int8
    property base_offset : Int64
    property last_offset_delta : Int32
    property producer_id : Int64
    property producer_epoch : Int8
    property base_sequence : Int32

    def initialize(@buffer : IO::Memory, @max_message_size : Int32, @debug_level : Int32, @orig_pos : Int32)
      @magic             = get(MAGIC_OFFSET)
      @count             = getInt(RECORDS_COUNT_OFFSET)
      @attributes        = getShort(ATTRIBUTES_OFFSET)
      @base_offset       = getLong(BASE_OFFSET_OFFSET)
      @last_offset_delta = getInt(LAST_OFFSET_DELTA_OFFSET)
      @producer_id       = getLong(PRODUCER_ID_OFFSET)
      @producer_epoch    = getShort(PRODUCER_EPOCH_OFFSET)
      @base_sequence     = getInt(BASE_SEQUENCE_OFFSET)
    end

    def last_offset : Int64
      base_offset + last_offset_delta
    end

    private def cur_pos
      @orig_pos + @buffer.pos
    end

    def each(&block : Record -> _)
      @buffer.seek(RECORD_BATCH_OVERHEAD)
      record_idx = 0
      return nil if count == 0

      record_idx += 1
      if compressed?
        raise "not implemented yet"
      else
        begin
          while remaining > 0
            position = @buffer.pos
            varint = Varint.from_kafka(@buffer)
            record_offset = position + varint.read_bytes
            record_length = varint.value
            bytes = @buffer.read_at(record_offset, record_length){|io| io.to_slice}
            @buffer.seek(record_offset + record_length)
            record = DefaultRecord.from_kafka(bytes, base_offset: base_offset, record_offset: record_offset, debug_level: @debug_level+1, orig_pos: @orig_pos + record_offset)
            block.call(record.as(Record))
          end
        rescue err
          logger.error err.to_s
        end
      end
    end
    
    def compression_type : CompressionType
      CompressionType.from_value(attributes & COMPRESSION_CODEC_MASK)
    end

    def compressed? : Bool
      ! compression_type.none?
    end

    private def remaining : Int32
      @max_message_size - @buffer.pos
    end

    private def get(offset : Int32) : Int8
      getShort(offset)
    end

    private def getShort(offset : Int32) : Int8
      @buffer.seek(offset)
      Int8.from_kafka(@buffer)
    end

    private def getInt(offset : Int32) : Int32
      @buffer.seek(offset)
      Int32.from_kafka(@buffer)
    end

    private def getLong(offset : Int32) : Int64
      @buffer.seek(offset)
      Int64.from_kafka(@buffer)
    end
  end

  def DefaultRecordBatch.from_kafka(bytes, debug_level, orig_pos)
    io = IO::Memory.new(bytes)
    title = "DefaultRecordBatch(%dbytes[%s..%s])" %
            [bytes.size, debug_addr(orig_pos), debug_addr(orig_pos + bytes.size)]
    debug title, color: :yellow
    new(io, bytes.size, debug_level, orig_pos)
  end
end
