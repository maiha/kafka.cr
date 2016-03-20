class Kafka
  module Commands
    module Fetch
      def fetch(topic : String, partition : Int32, offset : Int64, timeout : Time::Span = 1.second, min_bytes : Int32 = 0, max_bytes : Int32 = 1024)
        req = build_fetch_request(topic, partition, offset, timeout.milliseconds, min_bytes, max_bytes)
        res = execute(req, handler)
        #        res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res)
        return res
      end

      private def build_fetch_request(topic : String, partition : Int32, offset : Int64, timeout : Int32, min_bytes : Int32, max_bytes : Int32)
        replica = -1
        ps = Kafka::Protocol::Structure::FetchRequestPartitions.new(partition, offset, max_bytes)
        ts = [Kafka::Protocol::Structure::FetchRequestTopics.new(topic, [ps])]
        return Kafka::Protocol::FetchRequest.new(0, client_id, replica, timeout, min_bytes, ts)
      end
    end

    include Kafka::Commands::Fetch
  end
end
