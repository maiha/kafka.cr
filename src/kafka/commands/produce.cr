class Kafka
  module Commands
    module Produce
      include Kafka::Protocol

      class ProduceOption
        var required_acks : Int16, -1_i16
        var transactional_id : String, ""
        var timeout_ms : Int32, 1000
        var version : Int32, 0
        var partition : Int32, 0
      end
      
      def produce(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        case opt.version
        when 0 ; produce_v0(entry, datas, opt)
        when 1 ; produce_v1(entry, datas, opt)
        when 3 ; produce_v3(entry, datas, opt)
        else   ; raise NotImplemented.new("produce version=#{opt.version} not implemented yet")
        end
      end

      ######################################################################
      ### v0

      def produce_v0(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        res = raw_produce_v0(entry, datas, opt)
        return extract_produce_info!(res)
      end
  
      def raw_produce_v0(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        req = build_produce_request_v0(entry, datas, opt)
        res = fetch_produce_response(req)
        return res
      end

      private def build_produce_request_v0(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        tp = Structure::TopicAndPartitionMessages.new(entry, datas)
        Kafka::Protocol::ProduceV0Request.new(0, client_id, opt.required_acks, opt.timeout_ms.to_i32, [tp])
      end
      
      ######################################################################
      ### v1
      
      def produce_v1(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        res = raw_produce_v1(entry, datas, opt)
        return extract_produce_info!(res)
      end
  
      def raw_produce_v1(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        req = build_produce_request_v1(entry, datas, opt)
        res = fetch_produce_response(req)
        return res
      end
  
      private def build_produce_request_v1(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        tp = Structure::TopicAndPartitionMessages.new(entry, datas)
        Kafka::Protocol::ProduceV1Request.new(0, client_id, opt.required_acks, opt.timeout_ms.to_i32, [tp])
      end

      ######################################################################
      ### v3
      
      def produce_v3(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        res = raw_produce_v3(entry, datas, opt)
        return extract_produce_info!(res)
      end
  
      def raw_produce_v3(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        req = build_produce_request_v3(entry, datas, opt)
        res = fetch_produce_response(req)
        return res
      end
  
      private def build_produce_request_v3(entry : Kafka::Entry, datas : Array(Kafka::Data), opt : ProduceOption)
        tp = Structure::TopicAndPartitionMessages.new(entry, datas)
        Kafka::Protocol::ProduceV3Request.new(0, client_id, opt.transactional_id, opt.required_acks, opt.timeout_ms.to_i32, [tp])
      end

      private def fetch_produce_response(req)
        execute(req)
      end

      private def extract_produce_info!(res)
        res
      end
    end
  end
end
