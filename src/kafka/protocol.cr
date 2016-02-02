require "./protocol/*"

module Kafka::Protocol
  ######################################################################
  ### Api Code

  api  1, Fetch
  api  2, Offset
  api  3, Metadata
  api 12, Heartbeat

  ######################################################################
  ### Printing

  class MetadataResponse
    def to_s
      b = brokers.map(&.to_s).join(", ")
      t = topics.map(&.to_s).join(", ")
      <<-EOF
        brokers: #{b}
        topics: #{t}
        EOF
    end
  end
end
