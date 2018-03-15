require "./record_batch"

module Kafka::Protocol::Structure
  module Records
    include Enumerable(RecordBatch)

    OFFSET_OFFSET = 0
    OFFSET_LENGTH = 8
    SIZE_OFFSET   = OFFSET_OFFSET + OFFSET_LENGTH
    SIZE_LENGTH   = 4
    LOG_OVERHEAD  = SIZE_OFFSET + SIZE_LENGTH

    MAGIC_OFFSET = 16
    MAGIC_LENGTH = 1
    HEADER_SIZE_UP_TO_MAGIC = MAGIC_OFFSET + MAGIC_LENGTH
  end
end
