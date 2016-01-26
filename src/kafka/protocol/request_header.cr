require "./request"

module Kafka::Protocol
  class HeartbeatRequest < Request
    fields({
      key:           12,
      ver:           0,
      group_id:      String,
      generation_id: Int32,
      member_id:     String,
    })
  end
end
