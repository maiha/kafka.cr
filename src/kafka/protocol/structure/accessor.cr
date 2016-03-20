module Kafka::Protocol::Structure
  # #####################################################################
  # ## accessor

  class FetchResponsePartition
    delegate message_sets, "message_set_entry"
  end

  def Partition.build(p : Int32)
    Partition.new(p, latest_offset = -1_i64, max_offsets = 999999999)
  end

  class OffsetRequest
    def pretty_topic_partitions
      topic_partitions.reduce({} of String => Array(Int32)) { |hash, tap| hash[tap.topic] = tap.partitions.map(&.partition); hash }
    end
  end

  class PartitionOffset
    include Kafka::OffsetsReader
  end

  class MetadataResponse
    def broker_maps
      brokers.reduce({} of Int32 => Kafka::Broker) { |hash, b| hash[b.node_id] = Kafka::Broker.new(b.host, b.port); hash }
    end

    def broker!(id : Int32)
      broker_maps[id] || raise "[BUG] broker(#{id}) not found: meta=#{brokers.inspect}"
    end

    def to_offset_requests
      Builder::LeaderBasedOffsetRequestsBuilder.new(self).build
    end
  end
end
