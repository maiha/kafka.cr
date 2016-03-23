require "./protocol"
require "./commands/*"

class Kafka
  module Commands
    include Kafka::Protocol::Utils

    ######################################################################
    ### topic

    include Kafka::Commands::Topics

    # Returns the topic information
    #
    # Example:
    #
    # ```
    # kafka.topics # => [Kafka::TopicInfo, ...]
    # ```
    def topics(names : Array(String) = [] of String, consumer_offsets : Bool = false) : Array(Kafka::TopicInfo)
      opt = TopicsOption.new(consumer_offsets)
      topics(names, opt)
    end

    # Returns the topic information
    #
    # Example:
    #
    # ```
    # kafka.create_topic("t1", 1, 1)
    # ```
    def create_topic(name : String, partition : Int32, replication : Int32)
      raise "not implemented yet"
    end

    ######################################################################
    ### general

    def execute(request : Kafka::Request, handler : Kafka::Handler)
      Kafka::Execution.execute(connection, request, handler)
    end

    def execute(request : Kafka::Request)
      execute(request, handler)
    end
  end
end

