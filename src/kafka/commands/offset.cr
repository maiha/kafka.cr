class Kafka
  module Commands
    module Offset
      include Kafka::Protocol

      record OffsetOption, latest_offset, max_offsets

      def offset(topic : String, partition : Int32)
        idx = Kafka::Index.new(topic, partition, -1_i64)
        opt = OffsetOption.new(-1_i64, 999999999)
        offset(idx, opt)
      end

      def offset(index : Kafka::Index, opt : OffsetOption)
        res = offset_response(index, opt)
        return extract_offset!(index, res)
      end

      def offset_response(index : Kafka::Index, opt : OffsetOption)
        req = build_offset_request(index, opt)
        res = execute(req, handler)
        return res
      end

      protected def build_offset_request(index : Kafka::Index, opt : OffsetOption)
        replica = -1
        po = Structure::Partition.new(index.partition, opt.latest_offset, opt.max_offsets)
        taps = [Structure::TopicAndPartitions.new(index.topic, [po])]
        return OffsetRequest.new(0, client_id, replica, taps)
      end

      private def extract_offset!(index : Kafka::Index, res : OffsetResponse) : Kafka::Offset
        res.topic_partition_offsets.each do |ts|
          pos = ts.partition_offsets
          break if pos.empty?
          po = pos.first.not_nil!
          if po.error_code == 0
            return Kafka::Offset.new(index, po.offsets)
          else
            raise Kafka::OffsetNotFound.new(index, po.errmsg)
          end
        end
        raise Kafka::OffsetNotFound.new(index)
      end
    end

    include Kafka::Commands::Offset
  end
end
