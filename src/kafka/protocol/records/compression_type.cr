module Kafka::Protocol::Structure
  enum CompressionType
    NONE   = 0
    GZIP   = 1
    SNAPPY = 2
    LZ4    = 3
  end
end
