require "./record"

module Kafka::Protocol::Structure
  class DefaultRecord
    include Utils::GuessBinary

    property attributes : Int8
    property timestamp_delta : Varlong
    property offset_delta : Varint
    property key : Varbytes
    property val : Varbytes
    property headers : VarArray(Header)
    
    def initialize(@buffer : IO::Memory, @max_message_size : Int32, @base_offset : Int64, @debug_level : Int32, @orig_pos : Int32)
      @record_start    = 0
      @attributes      = Int8.from_kafka(@buffer, debug_level, :attributes, cur_pos)
      @timestamp_delta = Varlong.from_kafka(@buffer, debug_level, :timestamp_delta, cur_pos)
      #long timestamp = baseTimestamp + timestampDelta;

      # if (logAppendTime != null)
      #   timestamp = logAppendTime;

      @offset_delta = Varint.from_kafka(@buffer, debug_level, :offset_delta, cur_pos)
      @offset = Int64.new(@base_offset + @offset_delta.value)
      # int sequence = baseSequence >= 0 ?
      #   DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
      #   RecordBatch.NO_SEQUENCE;

      @key = Varbytes.from_kafka(@buffer, debug_level, :key, cur_pos)
      @val = Varbytes.from_kafka(@buffer, debug_level, :val, cur_pos)

      num_headers = Varint.from_kafka(@buffer, debug_level, :num_headers, cur_pos).value
      if num_headers < 0
        raise "InvalidRecordException: Found invalid number of record headers %s" % num_headers
      end

      @headers = VarArray(Header).new
      num_headers.times do
        header_key_size = Varint.from_kafka(@buffer, debug_level, :header_key_size, cur_pos).value
        if header_key_size < 0
          raise "InvalidRecordException: Invalid negative header key size %s" % header_key_size
        end

        header_key = String.new(@buffer.read_at(@buffer.pos, header_key_size, &.to_slice))
        debu_set_head_address(abs: cur_pos)
        title = "Bytes[#{header_key_size}](header_key) -> #{header_key}"
        debug title.colorize(:cyan)
        @buffer.seek(@buffer.pos + header_key_size)
      end
    end

    private def debug_level
      @debug_level
    end
    
    private def cur_pos
      @orig_pos + @buffer.pos
    end

    include Record

    def offset : Int64
      @base_offset + @offset_delta.value
    end

    def key : Bytes
      @key.bytes
    end

    def value : Bytes
      @val.bytes
    end

    def to_s(io : IO)
      value = guess_binary(self.value)
      value = pretty_binary(value.to_s)
      io << value
    end
  end

  def DefaultRecord.from_kafka(bytes, base_offset, record_offset, debug_level, orig_pos)
    io = IO::Memory.new(bytes)
    title = "DefaultRecord(%dbytes[%s..%s])" %
            [bytes.size, debug_addr(orig_pos), debug_addr(orig_pos + bytes.size)]
    debu_set_head_address(orig_pos)
    debug title.colorize(:yellow)
    new(io, bytes.size, base_offset: base_offset, debug_level: debug_level+1, orig_pos: orig_pos)
  end
end
