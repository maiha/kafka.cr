module Kafka::Protocol::Structure
  class Broker
    def to_s
      "(#{node_id} => #{host}:#{port})"
    end
  end

  class PartitionMetadata
    def to_s
      msg = errmsg(error_code, "{leader=#{leader},replica=#{replicas.inspect},isr=#{isrs.inspect}}")
      %(#{id} => #{msg})
    end
  end

  class TopicMetadata
    def to_s
      msg = errmsg(error_code, partitions.map(&.to_s).join(", "))
      %(#{name}(#{msg}))
    end
  end
end
