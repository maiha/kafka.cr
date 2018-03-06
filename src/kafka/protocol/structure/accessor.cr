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
      dst = IO::Memory.new(1 + 1 + key.bytesize + value.bytesize)
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
      crc32 = CRC32.checksum(slice).to_i32
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

  macro define_produce_response_methods(klass)
    class {{klass}}
      def error?
        topics.any?{|t| t.partitions.any?(&.error?)}
      end

      def errmsg
        topics.each do |t|
          t.partitions.each do |p|
            if p.error?
              return p.errmsg
            end
          end
        end
        return raise "no errors ({{klass}})"
      end
    end
  end

  define_produce_response_methods ProduceResponseV0
  define_produce_response_methods ProduceResponseV1
  define_produce_response_methods ProduceResponseV3

  macro define_fetch_response_methods(klass)
    class {{klass}}
      def messages!
        array = [] of Kafka::Message
        topics.each do |t|
          t.partitions.each do |p|
            if p.error?
              raise Kafka::Protocol::Error.new(p.error_code)
            end
            p.message_set_entry.message_sets.each do |s|
              index = Kafka::Index.new(t.topic, p.partition, s.offset)
              value = Kafka::Value.new(s.message.value)
              array << Kafka::Message.new(index, value)
            end
          end
        end
        return array
      end
    end
  end

  define_fetch_response_methods FetchResponse

  class FetchResponsePartition
    delegate message_sets, to: message_set_entry
  end

  def Partition.build(p : Int32)
    Partition.new(p, latest_offset = -1_i64, max_offsets = 999999999)
  end

  class ListOffsetsRequest
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
