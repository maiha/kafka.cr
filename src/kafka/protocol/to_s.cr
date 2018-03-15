module Kafka::Protocol::Structure
  class Broker
    def to_s(io : IO)
      io << "(#{node_id} => #{host}:#{port})"
    end
  end

  class PartitionMetadata
    def to_s(io : IO)
      msg = Kafka::Protocol.errmsg(error_code, "{leader=#{leader},replica=#{replicas.inspect},isr=#{isrs.inspect}}")
      io << "#{id} => #{msg}"
    end
  end

  class TopicMetadata
    def to_s(io : IO)
      msg = Kafka::Protocol.errmsg(error_code, partitions.map(&.to_s).join(", "))
      io << "#{name}(#{msg})"
    end
  end

  class RecordBatchV2
    def to_s(io : IO)
#      io << base_offset.to_s
      io << "(%d)" % records.size
      io << records.map(&.to_s).inspect
    end
  end
end
