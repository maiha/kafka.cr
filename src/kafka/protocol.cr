require "./protocol/*"

module Kafka::Protocol
  # See `Kafka::Api` for available protocol names
  protocol Produce, 0
  protocol Produce, 1
  protocol Produce, 3
  protocol Fetch
  protocol ListOffsets
  protocol Metadata
  protocol Metadata, 5
  protocol Heartbeat
  protocol InitProducerId
  protocol ApiVersions, 0
  protocol ApiVersions, 1
end
