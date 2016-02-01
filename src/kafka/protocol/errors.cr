module Kafka::Protocol
  enum Errors
    UnknownCode = -1
    NoError = 0
    OffsetOutOfRangeCode = 1
    InvalidMessageCode = 2
    UnknownTopicOrPartitionCode = 3
    InvalidFetchSizeCode  = 4
    LeaderNotAvailableCode = 5
    NotLeaderForPartitionCode = 6
    RequestTimedOutCode = 7
    BrokerNotAvailableCode = 8
    ReplicaNotAvailableCode = 9
    MessageSizeTooLargeCode = 10
    StaleControllerEpochCode = 11
    OffsetMetadataTooLargeCode = 12
    StaleLeaderEpochCode = 13
    OffsetsLoadInProgressCode = 14
    ConsumerCoordinatorNotAvailableCode = 15
    NotCoordinatorForConsumerCode = 16
    InvalidTopicCode = 17
    MessageSetSizeTooLargeCode = 18
    NotEnoughReplicasCode = 19
    NotEnoughReplicasAfterAppendCode = 20
    InvalidRequiredAcks = 21
    IllegalConsumerGeneration = 22
    InconsistentGroupProtocolCode = 23
    InvalidGroupIdCode = 24
    UnknownMemberIdCode = 25
    InvalidSessionTimeoutCode = 26
    RebalanceInProgressCode = 27
    InvalidCommitOffsetSizeCode = 28
    TopicAuthorizationCode = 29
    GroupAuthorizationCode = 30
    ClusterAuthorizationCode = 31
  end
end
