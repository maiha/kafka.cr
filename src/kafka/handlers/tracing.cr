module Kafka::Handlers
  class Tracing < Config
    property traces : Array(String)

    def initialize
      @traces    = [] of String
      @verbose   = false
      @request   = ->(req: Kafka::Request) { traces << "request"; nil }
      @send      = ->(bytes: Slice(UInt8)) { traces << "send" ; nil}
      @recv      = ->(bytes: Slice(UInt8)) { traces << "recv" ; nil}
      @respond   = ->(res: Kafka::Response) { traces << "respond" ; nil}
      @completed = ->(req: Kafka::Request, res: Kafka::Response) { traces << "completed" ; nil}
      @failed    = ->(req: Kafka::Request, err: Exception) { traces << "failed" ; nil}
    end

    def invoked?(handler_name : String)
      traces.includes?(handler_name)
    end
  end
end
