require "./protocol/*"

module Kafka::Protocol
  ######################################################################
  ### Api Code

  api  0, Produce, 0
  api  0, Produce, 1
  api  0, Produce, 3
  api  1, Fetch
  api  2, Offset   # old: ListOffsets
  api  3, Metadata
#  api  4, LeaderAndIsr
#  api  5, StopReplica
#  api  6, UpdateMetadata
#  api  7, ControlledShutdown
#  api  8, OffsetCommit
#  api  9, OffsetFetch
#  api 10, FindCoordinator # old: GroupCoordinator
#  api 11, JoinGroup
  api 12, Heartbeat
#  api 13, LeaveGroup
#  api 14, SyncGroup
#  api 15, DescribeGroups
#  api 16, ListGroups
#  api 17, SaslHandshake
#  api 18, ApiVersions
#  api 19, CreateTopics
#  api 20, DeleteTopics
#  api 21, DeleteRecords
  api 22, InitProducerId
#  api 23, OffsetForLeaderEpoch
#  api 24, AddPartitionsToTxn
#  api 25, AddOffsetsToTxn
#  api 26, EndTxn
#  api 27, WriteTxnMarkers
#  api 28, TxnOffsetCommit
#  api 29, DescribeAcls
#  api 30, CreateAcls
#  api 31, DeleteAcls
#  api 32, DescribeConfigs
#  api 33, AlterConfigs
#  api 34, AlterReplicaLogDirs
#  api 35, DescribeLogDirs
#  api 36, SaslAuthenticate
#  api 37, CreatePartitions
end
