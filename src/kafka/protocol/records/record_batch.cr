require "./record"

module Kafka::Protocol::Structure
  # https://github.com/apache/kafka/blob/1.0/clients/src/main/java/org/apache/kafka/common/record/DefaultRecord.java

  module RecordBatch
    include Enumerable(Record)

    MAGIC_VALUE_V0 = 0
    MAGIC_VALUE_V1 = 1
    MAGIC_VALUE_V2 = 2

    # The current "magic" value
    CURRENT_MAGIC_VALUE = MAGIC_VALUE_V2
  end
end
