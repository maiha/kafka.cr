module Kafka::Protocol::Structure
  module Builder
    class LeaderBasedOffsetRequestsBuilder
      getter req
      property! name
      property! replica
      property! latest_offset : Int64
      property! max_num_offsets : Int32
      
      record LeaderTopicPartition,
        leader : Int32,
        topic : String,
        partition : Int32
      
      def initialize(@req : MetadataResponseV0)
        @name = "LeaderBasedOffsetRequestsBuilder"
        @replica = -1
        @latest_offset = -1.to_i64
        @max_num_offsets = 999999999
      end

      def build # : Hash(Int32, Kafka::Protocol::ListOffsetsRequestV0)
        ({} of Int32 => Kafka::Protocol::ListOffsetsRequestV0).tap{ |hash|
          grouped_leader_topic_partitions.each do |leader, taps|
            hash[leader] = Kafka::Protocol::ListOffsetsRequestV0.new(0, name, replica, taps)
          end
        }
      end

      private def leader_topic_partitions : Array(LeaderTopicPartition)
        array = [] of LeaderTopicPartition
        req.topics.each do |mt|
          mt.partitions.each do |mp|
            array << LeaderTopicPartition.new(mp.leader, mt.name, mp.id)
          end
        end
        return array
      end

      private def grouped_leader_topic_partitions
        hash = {} of Int32 => Array(TopicAndPartitions)

        ltps = leader_topic_partitions
        # Hash(Int32, Array(TopicPartitions))

        ltps.group_by{|x| [x.leader, x.topic]}.each do |lt, ary|
          # [2, "a"] => [TAP(@leader=2, @topic="b", @partition=0)]
          leader = lt[0].not_nil!.to_i # 2
          topic  = lt[1].not_nil!.to_s # "a"
          hash[leader] ||= [] of TopicAndPartitions
          hash[leader] << TopicAndPartitions.new(topic, ary.map{|p| Partition.new(p.partition, latest_offset, max_num_offsets)})
        end

        return hash
      end
    end
  end
end
