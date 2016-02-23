module Kafka::Protocol::Structure
  module Builder
    class LeaderBasedOffsetRequestsBuilder
      getter req
      property! name
      property! replica
      
      record LeaderTopicPartition, leader, topic, partition
      
      def initialize(@req : MetadataResponse)
        @name = "LeaderBasedOffsetRequestsBuilder"
        @replica = -1
      end

      def build # : Hash(Int32, OffsetRequest)
        ({} of Int32 => OffsetRequest).tap{ |hash|
          grouped_leader_topic_partitions.each do |leader, taps|
            hash[leader] = Kafka::Protocol::OffsetRequest.new(0, name, replica, taps)
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
          hash[leader] << TopicAndPartitions.new(topic, ary.map{|p| Partition.build(p.partition)})
        end

        return hash
      end
    end
  end
end
