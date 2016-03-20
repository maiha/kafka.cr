module Kafka::Handlers
  class Base
    include Kafka::Handler

    def verbose : Bool
      false
    end

    def request(request : Kafka::Request)
    end

    def request(bytes : Slice(UInt8))
    end

    def send(bytes : Slice(UInt8))
    end

    def recv(bytes : Slice(UInt8))
    end

    def respond(response : Kafka::Response)
    end

    def completed(request : Kafka::Request, response : Kafka::Response)
    end

    def failed(request : Kafka::Request, err)
    end
  end
end
