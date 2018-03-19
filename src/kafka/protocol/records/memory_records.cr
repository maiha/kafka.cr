require "./records"

module Kafka::Protocol::Structure
  class MemoryRecords
    include Records

    def initialize(@buffer : IO::Memory, @max_message_size : Int32, @orig_pos : Int32, @debug_level = -1)
      @batch_pos = 0
      @each_index = -1

      # eagar loading
      each(&.to_s) if Kafka.debug?
    end

    def each(&block : RecordBatch -> _)
      @buffer.rewind
      @each_index = -1

      while batch = next_batch?
        block.call(batch.as(RecordBatch))
      end
    end

    private def next_batch? : RecordBatch?
      @each_index += 1
      @batch_pos = @buffer.pos
      group = "MemoryRecords#next_batch[#{@each_index}]"
      
      next_iterate_pos = @buffer.pos
      begin
        if remaining < LOG_OVERHEAD
          debug2 "finished (remaining < LOG_OVERHEAD)[#{remaining},#{LOG_OVERHEAD}]", group: group
          return nil
        end

        @buffer.seek(@batch_pos + SIZE_OFFSET)
        debug "[8](OFFSET)", color: :cyan, prefix: cur_pos
        
        record_size = Int32.from_kafka(@buffer)
        debug2 "record_size=#{record_size}", group: group

        next_iterate_pos = @batch_pos + LOG_OVERHEAD + record_size

        @buffer.seek(@batch_pos)
        
        if record_size < LegacyRecord::RECORD_OVERHEAD_V0
          raise "CorruptRecordException: Record size is less than the minimum record overhead (%d)" % LegacyRecord::RECORD_OVERHEAD_V0
        end

        if record_size > @max_message_size
          raise "CorruptRecordException: Record size exceeds the largest allowable message size (%d)." % @max_message_size
        end

        batch_size = record_size + LOG_OVERHEAD
        if remaining < batch_size
          debug2 "finished (remaining < batch_size)[#{remaining},#{batch_size}]", group: group
          return nil
        end

        @buffer.seek(@batch_pos + MAGIC_OFFSET)
        magic = Int8.from_kafka(@buffer)
        debug2 "magic=#{magic}", group: group
        
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
      rescue err
        logger.error "ERROR: #{group}: #{err}"
        return nil
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

    private def absolute_pos : Int32
      @orig_pos + @batch_pos
    end

    private def debug_level
      @debug_level
    end
    
    private def cur_pos
      @orig_pos + @buffer.pos
    end
  end

  def MemoryRecords.from_kafka(io : IO, debug_level = -1, hint = "")
    pos = io.pos
    byte_size = Int32.from_kafka(io)
    orig_pos = io.pos
    
    debug "RECORDS[4] -> #{byte_size} bytes", color: :yellow, prefix: debug_address(abs: pos)

    bytes = Bytes.new(byte_size)
    io.read_fully(bytes)

    debug "(MemoryRecords)", color: :yellow
    return new(IO::Memory.new(bytes), bytes.size, orig_pos, debug_level+1)
  end
end
