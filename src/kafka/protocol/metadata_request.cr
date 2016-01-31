require "./structure/*"

class Kafka::Protocol::MetadataRequest < Kafka::Protocol::Structure::MetadataRequest
  request 3, 0
end
