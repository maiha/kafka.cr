class Kafka
  module Commands
    module Topics
      include Kafka::Protocol

      record TopicsOption, consumer_offsets

      def topics(names : Array(String), opt : TopicsOption)
        req = build_topics_request(names)
        res = fetch_topics_response(req)
        return extract_message!(req, res, opt)
      end
  
      private def build_topics_request(topics : Array(String))
        Kafka::Protocol::MetadataRequest.new(0, client_id, topics)
      end

      private def fetch_topics_response(req)
        execute(req, handler)
      end

      private def extract_message!(req, res, opt)
        results = [] of Kafka::TopicInfo
        res.topics.each do |meta|
          if meta.error_code == 0
            if !opt.consumer_offsets && meta.name == "__consumer_offsets"
              # skip
            else
              meta.partitions.each do |pm|
                if pm.error_code == 0
                  results << Kafka::TopicInfo.new(meta.name, pm.id, pm.leader, pm.replicas, pm.isrs)
                else
                  err = Kafka::Protocol.errmsg(pm.error_code)
                  STDOUT.puts "#{meta.name}##{pm.id}\t#{err}"
                end
              end
            end
          else
            STDERR.puts "ERROR: #{meta.to_s}"
          end
        end
        return results
      end
    end
  end
end
