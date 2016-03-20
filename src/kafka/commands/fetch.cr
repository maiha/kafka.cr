class Kafka
  module Commands
    module Fetch
      include Kafka::Protocol

      record FetchOption, timeout, min_bytes, max_bytes

      def fetch(topic : String, partition : Int32, offset : Int64, timeout : Time::Span = 1.second, min_bytes : Int32 = 0, max_bytes : Int32 = 1024)
        idx = Kafka::Index.new(topic, partition, offset)
        opt = FetchOption.new(timeout, min_bytes, max_bytes)
        fetch(idx, opt)
      end

      def fetch(index : Kafka::Index, opt : FetchOption)
        res = fetch_response(index, opt)
        return extract_message!(index, res)
      end

      def fetch_response(index : Kafka::Index, opt : FetchOption)
        req = build_fetch_request(index, opt)
        res = execute(req, handler)
        #        res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res)
        return res
      end

      private def extract_message!(index : Kafka::Index, res) : Kafka::Message
        res.topics.each do |ts|
          ts.partitions.each do |ps|
            sets = ps.message_set_entry.message_sets
            break if sets.empty?
            value = Kafka::Value.new(sets.first.not_nil!.message.value)
            return Kafka::Message.new(index, value)
          end
        end
        raise Kafka::MessageNotFound.new(index)
      end
  
      private def build_fetch_request(index : Kafka::Index, opt : FetchOption)
        replica = -1
        ps = Structure::FetchRequestPartitions.new(index.partition, index.offset, opt.max_bytes)
        ts = [Structure::FetchRequestTopics.new(index.topic, [ps])]
        return FetchRequest.new(0, client_id, replica, opt.timeout.milliseconds, opt.min_bytes, ts)
      end
    end

    include Kafka::Commands::Fetch
  end
end
