require "zlib"

module Kafka::Protocol::Structure
  Null = Slice(UInt8).new(0)

  def null
    Null
  end
  
  class Message
    def initialize(value : Bytes)
      initialize(Null, value)
    end

    def initialize(key : Bytes, value : Bytes)
      initialize(0_i8, 0_i8, key, value)
    end
    
    def initialize(magic_byte : Int8, attributes : Int8, key : Bytes, value : Bytes)
      dst = MemoryIO.new(1 + 1 + key.bytesize + value.bytesize)
      magic_byte.to_kafka(dst)
      attributes.to_kafka(dst)
      if key == Null
        -1.to_kafka(dst)
      else
        key.to_kafka(dst)
      end
      if value == Null
        -1.to_kafka(dst)
      else
        value.to_kafka(dst)
      end
      slice = dst.to_slice
      crc32 = Zlib.crc32(slice).to_i32
      initialize(crc32, magic_byte, attributes, key, value)
    end
  end

  class MessageSet
    def initialize(offset : Int64, message : Message)
      bytesize = message.to_slice.bytesize
      initialize(offset, bytesize, message)
    end

    # to produce
    def initialize(message : Message)
      initialize(0_i64, message)
    end

    def initialize(data : Kafka::Data)
      initialize(data.body)
    end

    def initialize(body : Bytes)
      initialize(Message.new(body))
    end

    def initialize(body : String)
      initialize(Message.new(body.to_slice))
    end
  end

  class TopicAndPartitionMessages
    def initialize(entry : Kafka::Entry, data : Kafka::Data)
      ms = MessageSetEntry.new([MessageSet.new(data.body)])
      pm = PartitionMessage.new(entry.partition, ms)
      initialize(entry.topic, [pm])
    end

    def initialize(entry : Kafka::Entry, datas : Array(Kafka::Data))
      ms = MessageSetEntry.new(datas.map{|d| MessageSet.new(d)})
      pm = PartitionMessage.new(entry.partition, ms)
      initialize(entry.topic, [pm])
    end
  end
  
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
