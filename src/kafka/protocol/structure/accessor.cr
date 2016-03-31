require "zlib"

module Kafka::Protocol::Structure
  ######################################################################
  ### constructor

  def null
    Slice(UInt8).new(0)
  end
  
  class Message
    def initialize(key : Bytes, value : Bytes)
      bytes = Message.new(0, 0.to_i8, 0.to_i8, key, value).to_slice
      slice = Slice(UInt8).new(bytes.bytesize - 4){|i| bytes[i]}
      crc32 = Zlib.crc32(slice).to_i32
      initialize(crc32, 0.to_i8, 0.to_i8, key, value)
    end

    def initialize(value : Bytes)
      initialize(Slice(UInt8).new(0), value)
    end
  end

  class MessageSet
    def initialize(offset : Int64, message : Message)
      bytesize = message.to_slice.bytesize
      initialize(offset, bytesize, message)
    end

    # to produce
    def initialize(message : Message)
      initialize(-1_i64, message)
    end

    def initialize(body : Bytes)
      initialize(Message.new(body))
    end
  end

  class TopicAndPartitionMessages
    def initialize(entry : Kafka::Entry, data : Kafka::Data)
      ms = MessageSetEntry.new([MessageSet.new(data.body)])
      pm = PartitionMessage.new(entry.partition, ms)
      initialize(entry.topic, [pm])
    end
  end
  
  ######################################################################
  ### accessor

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
