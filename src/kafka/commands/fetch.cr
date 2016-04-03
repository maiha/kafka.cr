class Kafka
  module Commands
    module Fetch
      include Kafka::Protocol

      class FetchOption
        var topic : String, ""
        var partition : Int32, 0
        var offset : Int64, 0_i64
        var timeout : Time::Span, 1.second
        var min_bytes : Int32, 0
        var max_bytes : Int32, 1024

        def index
          Kafka::Index.new(topic, partition, offset)
        end
      end

      def fetch(opt : FetchOption)
        res = fetch_response(opt)
        messages = res.messages!
        if messages.any?
          return messages.first.not_nil!
        else
          raise Kafka::MessageNotFound.new(opt.index)
        end
      end

      def fetch_response(opt : FetchOption)
        req = build_fetch_request(opt)
        res = execute(req)
        #        res = execute(req, resolve_leader!(topic, partition)) if not_leader?(res)
        return res
      end

      private def build_fetch_request(opt : FetchOption)
        replica = -1
        ps = Structure::FetchRequestPartitions.new(opt.partition, opt.offset, opt.max_bytes)
        ts = [Structure::FetchRequestTopics.new(opt.topic, [ps])]
        return FetchRequest.new(0, client_id, replica, opt.timeout.milliseconds, opt.min_bytes, ts)
      end
    end
  end
end
