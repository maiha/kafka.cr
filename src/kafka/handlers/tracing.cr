module Kafka::Handlers
  class Tracing < Config
    property traces : Array(String)

    def initialize
      @traces    = [] of String
      @verbose   = false
      @request   = ->(req: Kafka::Request) { traces << "request" }
      @send      = ->(bytes: Slice(UInt8)) { traces << "send" }
      @recv      = ->(bytes: Slice(UInt8)) { traces << "recv" }
      @respond   = ->(res: Kafka::Response) { traces << "respond" }
      @completed = ->(req: Kafka::Request, res: Kafka::Response) { traces << "completed" }
      @failed    = ->(req: Kafka::Request, err: Exception) { traces << "failed" }
    end

    def invoked?(handler_name : String)
      traces.includes?(handler_name)
    end
  end
end
