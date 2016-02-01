require "./structure/*"

module Kafka::Protocol
  class MetadataRequest < Structure::MetadataRequest
    request 3, 0
  end

  class MetadataResponse < Structure::MetadataResponse
    response

    def to_s
      b = "{" + brokers.map { |b| "#{b.node_id} => #{b.host}:#{b.port}" }.join(", ") + "}"
      t = topics.map(&.to_s).join(", ")
      <<-EOF
        brokers: #{b}
        topics: #{t}
        EOF
    end
  end
end
