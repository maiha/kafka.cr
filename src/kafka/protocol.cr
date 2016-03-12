require "./protocol/*"

module Kafka::Protocol
  ######################################################################
  ### Api Code

  api  0, Produce
  api  1, Fetch
  api  2, Offset
  api  3, Metadata
#  api  4, LeaderAndIsr
#  api  5, StopReplica
#  api  6, UpdateMetadata
#  api  7, ControlledShutdown
#  api  8, OffsetCommit
#  api  9, OffsetFetch
#  api 10, GroupCoordinator
#  api 11, JoinGroup
  api 12, Heartbeat
#  api 13, LeaveGroup
#  api 14, SyncGroup
#  api 15, DescribeGroups
#  api 16, ListGroups
  
  ######################################################################
  ### Printing

  class MetadataResponse
    def to_s
      b = brokers.map(&.to_s).join(", ")
      t = topics.map(&.to_s).join(", ")
      <<-EOF
        brokers: #{b}
        topics: #{t}
        EOF
    end
  end
end
