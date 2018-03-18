require "./records"

module Kafka::Protocol::Structure
  class MemoryRecords
    include Records

    def initialize(@buffer : IO::Memory, @max_message_size : Int32, @orig_pos : Int32, @debug_level = -1)
      @batch_pos = 0

      # eagar loading
      each(&.to_s) if Kafka.debug?
    end

    def each(&block : RecordBatch -> _)
      @buffer.rewind

      while batch = next_batch?
        block.call(batch.as(RecordBatch))
      end
    end

    private def absolute_pos : Int32
      @orig_pos + @batch_pos
    end

    private def debug_level
      @debug_level
    end
    
    private def cur_pos
      @orig_pos + @buffer.pos
    end

    private def next_batch? : RecordBatch?
      @batch_pos = @buffer.pos
      next_iterate_pos = @buffer.pos
      begin
        return nil if (remaining < LOG_OVERHEAD)

#        on_debug_head_address(abs: cur_pos)
#        on_debug "[8](OFFSET)".colorize(:cyan)
        @buffer.seek(@batch_pos + SIZE_OFFSET)

#        record_size = Int32.from_kafka(@buffer, @debug_level, :record_size, @orig_pos)
        record_size = Int32.from_kafka(@buffer)
        next_iterate_pos = @batch_pos + LOG_OVERHEAD + record_size

        if record_size < LegacyRecord::RECORD_OVERHEAD_V0
          raise "CorruptRecordException: Record size is less than the minimum record overhead (%d)" % LegacyRecord::RECORD_OVERHEAD_V0
        end

        if record_size > @max_message_size
          raise "CorruptRecordException: Record size exceeds the largest allowable message size (%d)." % @max_message_size
        end

        batch_size = record_size + LOG_OVERHEAD
        return nil if remaining < batch_size

        @buffer.seek(@batch_pos + MAGIC_OFFSET)
#        magic = Int8.from_kafka(@buffer, @debug_level, :magic, @orig_pos)
        magic = Int8.from_kafka(@buffer)

        if magic < 0 || magic > RecordBatch::CURRENT_MAGIC_VALUE
          raise "CorruptRecordException: Invalid magic found in record: %s" % magic
        end

        batch_bytes = @buffer.read_at(@batch_pos, batch_size){|io| io.to_slice}
        @buffer.seek(@batch_pos + batch_size)
      
        if magic > RecordBatch::MAGIC_VALUE_V1
          return DefaultRecordBatch.from_kafka(batch_bytes, @debug_level, @orig_pos + @batch_pos)
        else
          return AbstractLegacyRecordBatch.new(IO::Memory.new(batch_bytes))
        end
      ensure
        @buffer.seek(next_iterate_pos)
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
    pos = io.pos
    byte_size = Int32.from_kafka(io)
    orig_pos = io.pos

    on_debug_head_address(abs: pos)
    on_debug "RECORDS[4] -> #{byte_size} bytes".colorize(:yellow)

    bytes = Bytes.new(byte_size)
    io.read_fully(bytes)

    on_debug_head_padding
    on_debug "(MemoryRecords)".colorize(:yellow)
    return new(IO::Memory.new(bytes), bytes.size, orig_pos, debug_level+1)
  end
end
