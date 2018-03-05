class Kafka
  module Commands
    module InitProducerId
      include Kafka::Protocol

      record InitProducerIdOption, consumer_offsets : Bool

      def init_producer_id(transactional_id : String, transaction_timeout_ms : Int32)
        req = build_init_producer_id_request(transactional_id, transaction_timeout_ms)
        res = fetch_init_producer_id_response(req)
        return res
      end
  
      private def build_init_producer_id_request(transactional_id : String, transaction_timeout_ms : Int32)
        Kafka::Protocol::InitProducerIdRequest.new(0, client_id, transactional_id, transaction_timeout_ms)
      end

      private def fetch_init_producer_id_response(req)
        execute(req)
      end
    end
  end
end
