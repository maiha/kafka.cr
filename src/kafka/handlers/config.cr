module Kafka::Handlers
  class Config
    include Kafka::Handler

    property! verbose
    property! request
    property! send
    property! recv
    property! respond
    property! completed
    property! failed

    def initialize
      @verbose   = false
      @request   = ->(req: Kafka::Protocol::Request) {}
      @send      = ->(bytes: Slice(UInt8)) {}
      @recv      = ->(bytes: Slice(UInt8)) {}
      @respond   = ->(res: Kafka::Protocol::Response) {}
      @completed = ->(req: Kafka::Protocol::Request, res: Kafka::Protocol::Response) {}
      @failed    = ->(req: Kafka::Protocol::Request, err: Exception) {}
    end

    def request(req : Kafka::Protocol::Request)
      @request.call(req)
    end

    def send(bytes : Slice(UInt8))
      @send.call(bytes)
    end

    def recv(bytes : Slice(UInt8))
      @recv.call(bytes)
    end

    def respond(res : Kafka::Protocol::Response)
      @respond.call(res)
    end

    def completed(req : Kafka::Protocol::Request, res : Kafka::Protocol::Response)
      @completed.call(req, res)
    end

    def failed(req : Kafka::Protocol::Request, err : Exception)
      @failed.call(req, err)
    end
  end
end
