require "./records"

module Kafka::Protocol::Structure
  class MemoryRecords
    include Records

    def initialize(@buffer : IO::Memory, @max_message_size : Int32)
    end

    def each(&block : RecordBatch -> _)
      @buffer.rewind
      while batch = next_batch?
        block.call(batch.as(RecordBatch))
      end
    end

    private def next_batch? : RecordBatch?
      position = @buffer.pos
      return nil if (remaining < LOG_OVERHEAD)

      @buffer.seek(position + SIZE_OFFSET)
      record_size = Int32.from_kafka(@buffer)

      if record_size < LegacyRecord::RECORD_OVERHEAD_V0
        raise "CorruptRecordException: Record size is less than the minimum record overhead (%d)" % LegacyRecord::RECORD_OVERHEAD_V0
      end

      if record_size > @max_message_size
        raise "CorruptRecordException: Record size exceeds the largest allowable message size (%d)." % @max_message_size
      end

      batch_size = record_size + LOG_OVERHEAD
      return nil if remaining < batch_size

      @buffer.seek(position + MAGIC_OFFSET)
      magic = Int8.from_kafka(@buffer)

      batch_bytes = @buffer.read_at(position, batch_size){|io| io.to_slice}
      @buffer.seek(position + batch_size)

      if magic < 0 || magic > RecordBatch::CURRENT_MAGIC_VALUE
        raise "CorruptRecordException: Invalid magic found in record: %s" % magic
      end
        
      if magic > RecordBatch::MAGIC_VALUE_V1
        return DefaultRecordBatch.new(batch_bytes)
      else
        return AbstractLegacyRecordBatch.new(IO::Memory.new(batch_bytes))
      end
    end

    def to_kafka(io : IO)
      raise "NotImplementedYet"
    end

    private def remaining : Int32
      @max_message_size - @buffer.pos
    end
  end

  def MemoryRecords.from_kafka(io : IO, debug_level = -1, hint = "")
    on_debug_head_padding

    byte_size = Int32.from_kafka(io, debug_level_succ, :size)
    bytes = Bytes.new(byte_size)
    io.read_fully(bytes)

    on_debug "RECORDS[2] -> #{byte_size} bytes".colorize(:yellow)
    on_debug_head_address
    
    records = new(IO::Memory.new(bytes), bytes.size)
    return records
  end
end
