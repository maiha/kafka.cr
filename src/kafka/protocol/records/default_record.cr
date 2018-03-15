require "./record"

module Kafka::Protocol::Structure
  class DefaultRecord
    property attributes : Int8
    property timestamp_delta : Varlong
    property offset_delta : Varint
    property key : Varbytes
    property val : Varbytes
    property headers : VarArray(Header)
    
    def initialize(@bytes : Bytes, @base_offset : Int64)
      @buffer = IO::Memory.new(@bytes)
      @max_message_size = Int32.new(@bytes.size)
      
      @record_start    = 0
      @attributes      = Int8.from_kafka(@buffer)
      @timestamp_delta = Varlong.decode(@buffer)
      #long timestamp = baseTimestamp + timestampDelta;

      # if (logAppendTime != null)
      #   timestamp = logAppendTime;

      @offset_delta = Varint.decode(@buffer)
      @offset = Int64.new(@base_offset + @offset_delta.value)
      # int sequence = baseSequence >= 0 ?
      #   DefaultRecordBatch.incrementSequence(baseSequence, offsetDelta) :
      #   RecordBatch.NO_SEQUENCE;

      @key = Varbytes.from_kafka(@buffer)
      @val = Varbytes.from_kafka(@buffer)

      num_headers = Varint.decode(@buffer).value
      if num_headers < 0
        raise "InvalidRecordException: Found invalid number of record headers %s" % num_headers
      end
      
      @headers = VarArray(Header).new
      num_headers.times do
        header_key_size = Varint.decode(@buffer).value
        if header_key_size < 0
          raise "InvalidRecordException: Invalid negative header key size %s" % header_key_size
        end
        
        header_key = String.new(@buffer.read_at(@buffer.pos, header_key_size, &.to_slice))
        @buffer.seek(@buffer.pos + header_key_size)
      end
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
  end
end
