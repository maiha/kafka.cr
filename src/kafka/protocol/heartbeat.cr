require "./structure/*"

module Kafka::Protocol
  class HeartbeatRequest < Structure::HeartbeatRequest
    request 12, 0
  end

  class HeartbeatResponse < Structure::HeartbeatResponse
    response
  end
end
