require "./protocol"

class Kafka
  module Commands
    include Kafka::Protocol::Utils

    def execute(request : Kafka::Protocol::Request, handler : Kafka::Handler)
      Kafka::Execution.execute(connection, request, handler)
    end

    def execute(request : Kafka::Protocol::Request)
      execute(request, handler)
    end
  end
end

require "./commands/*"
