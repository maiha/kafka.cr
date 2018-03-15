require "./record_batch"

module Kafka::Protocol::Structure
  class AbstractLegacyRecordBatch
    include RecordBatch

    def initialize(@buffer : IO::Memory)
    end

    def each(&block)
      raise "not implemented yet"
    end
  end
end
