require "./protocol/*"

module Kafka::Protocol
  class HeartbeatRequest < Structure::HeartbeatRequest
    request 12, 0
  end

  class HeartbeatResponse < Structure::HeartbeatResponse
    response
  end

  class MetadataRequest < Structure::MetadataRequest
    request 3, 0
  end

  class MetadataResponse < Structure::MetadataResponse
    response

    def to_s
      partition_metadata = ->(x : PartitionMetadata) {
        msg = errmsg(x.error_code, "{leader=#{x.leader},replica=#{x.replicas.inspect},isr=#{x.isrs.inspect}}")
        %(#{x.id} => #{msg})
      }

      b = "{" + brokers.map { |b| "#{b.node_id} => #{b.host}:#{b.port}" }.join(", ") + "}"
      t = topics.map{|t|
        # t1(0 => {leader=1,replica=[1],isr=[1]})
        %(#{t.name}(#{errmsg(t.error_code, t.partitions.map{|pm| partition_metadata.call(pm)}.join(", "))}))
      }.join(", ")
      <<-EOF
        brokers: #{b}
        topics: #{t}
        EOF
    end
  end

  class OffsetRequest < Structure::OffsetRequest
    request 2, 0
  end

  class OffsetResponse < Structure::OffsetResponse
    response
  end
end
