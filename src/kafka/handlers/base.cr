module Kafka::Handlers
  class Base
    include Kafka::Handler

    def verbose : Bool
      false
    end

    def request(request : Kafka::Protocol::Request)
    end

    def request(bytes : Slice(UInt8))
    end

    def send(bytes : Slice(UInt8))
    end

    def recv(bytes : Slice(UInt8))
    end

    def respond(response : Kafka::Protocol::Response)
    end

    def completed(request : Kafka::Protocol::Request, response : Kafka::Protocol::Response)
    end

    def failed(request : Kafka::Protocol::Request, err)
    end
  end
end
