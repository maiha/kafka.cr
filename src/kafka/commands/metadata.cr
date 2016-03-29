class Kafka
  module Commands
    module Metadata
      include Kafka::Protocol

      record MetadataOption, consumer_offsets
      def MetadataOption.zero
        MetadataOption.new(false)
      end

      def metadata(topics : Array(String), opt : MetadataOption) : Kafka::MetadataInfo
        res = raw_metadata(topics, opt)
        return extract_metadata_info!(res)
      end
  
      def raw_metadata(topics : Array(String), opt : MetadataOption) : MetadataResponse
        req = build_metadata_request(topics)
        res = fetch_metadata_response(req)
        return res
      end
  
      private def build_metadata_request(topics : Array(String))
        Kafka::Protocol::MetadataRequest.new(0, client_id, topics)
      end

      private def fetch_metadata_response(req)
        execute(req)
      end

      private def extract_metadata_info!(res : MetadataResponse)
        brokers = res.brokers.map{|b| Kafka::Broker.new(b.host, b.port)}
        MetadataInfo.new(brokers)
      end
    end
  end
end
