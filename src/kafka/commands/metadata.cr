class Kafka
  module Commands
    module Metadata
      include Kafka::Protocol

      record MetadataOption, consumer_offsets : Bool

      def MetadataOption.zero
        MetadataOption.new(false)
      end

      def metadata(topics : Array(String), opt : MetadataOption) : Kafka::MetadataInfo
        res = raw_metadata(topics, opt)
        return extract_metadata_info!(res)
      end
  
      def raw_metadata(topics : Array(String), opt : MetadataOption) : MetadataResponseV0
        req = build_metadata_request(topics)
        res = fetch_metadata_response(req)
        return res
      end
  
      private def build_metadata_request(topics : Array(String))
        Kafka::Protocol::MetadataRequestV0.new(0, client_id, topics)
      end

      private def fetch_metadata_response(req)
        execute(req)
      end

      private def extract_metadata_info!(res : MetadataResponseV0)
        brokers = res.brokers.map{|b| Kafka::Broker.new(b.host, b.port)}
        topics  = res.topics.map{|t|
          if t.error?
            Kafka::TopicError.new(t.name, -1, t.error_code, t.errmsg)
          else
            t.partitions.map{|p|
              if p.error?
                Kafka::TopicError.new(t.name, p.id, p.error_code, p.errmsg)
              else
                Kafka::TopicInfo.new(t.name, p.id, p.leader, p.replicas, p.isrs)
              end
            }
          end
        }.flatten
        MetadataInfo.new(brokers, topics)
      end
    end
  end
end
