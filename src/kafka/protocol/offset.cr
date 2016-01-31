require "./structure/*"

module Kafka::Protocol
  class OffsetRequest < Structure::OffsetRequest
    request 2, 0
  end
end
