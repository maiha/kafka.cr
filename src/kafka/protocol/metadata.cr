require "./request"

module Kafka::Protocol
  class MetadataRequest < Request
    request 3

    response MetadataResponse
  end

  class MetadataResponse
  end
end
