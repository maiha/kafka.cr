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

    def request(request : Kafka::Request)
      traces << "request(Kafka::Request)"
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

    def respond(response : Kafka::Response)
      traces << "respond(Kafka::Response)"
    end

    def completed(request : Kafka::Request, response : Kafka::Response)
      traces << "completed(Kafka::Request,Kafka::Response)"
    end

    def failed(request : Kafka::Request, err : Exception)
      traces << "failed(Kafka::Request,Exception)"
    end
  end
end
