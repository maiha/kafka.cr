module Kafka::Handlers
  class Tracing
    include Kafka::Handler

    property traces : Array(String)

    def initialize
      @traces = [] of String
    end

    def invoked?(handler_name : String)
      traces.grep(/^#{handler_name}\b/).any?
    end

    # not worked yet
    macro trace(name, props)
      def {{name.id}}({{props}})
        traces << {{name.id.stringify}}
      end
    end

    def verbose
      false
    end

    def request(request : Kafka::Protocol::Request)
      traces << "request(Kafka::Protocol::Request)"
    end

    def request(bytes : Slice(UInt8))
      traces << "request(Slice(UInt8))"
    end

    def send(bytes : Slice(UInt8))
      traces << "send(Slice(UInt8))"
    end

    def recv(bytes : Slice(UInt8))
      traces << "recv(Slice(UInt8))"
    end

    def respond(response : Kafka::Protocol::Response)
      traces << "respond(Kafka::Protocol::Response)"
    end

    def completed(request : Kafka::Protocol::Request, response : Kafka::Protocol::Response)
      traces << "completed(Kafka::Protocol::Request,Kafka::Protocol::Response)"
    end

    def failed(request : Kafka::Protocol::Request, err : Exception)
      traces << "failed(Kafka::Protocol::Request,Exception)"
    end
  end
end
