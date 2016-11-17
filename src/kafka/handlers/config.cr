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

    @request   : Proc(Kafka::Request, Nil)
    @send      : Proc(Bytes, Nil)
    @recv      : Proc(Bytes, Nil)
    @respond   : Proc(Kafka::Response, Nil)
    @completed : Proc(Kafka::Request, Kafka::Response, Nil)
    @failed    : Proc(Kafka::Request, Exception, Nil)

    def initialize
      @verbose   = false
      @request   = ->(req: Kafka::Request) {}
      @send      = ->(bytes: Slice(UInt8)) {}
      @recv      = ->(bytes: Slice(UInt8)) {}
      @respond   = ->(res: Kafka::Response) {}
      @completed = ->(req: Kafka::Request, res: Kafka::Response) {}
      @failed    = ->(req: Kafka::Request, err: Exception) {}
    end

    def request(req : Kafka::Request)
      @request.call(req)
    end

    def send(bytes : Slice(UInt8))
      @send.call(bytes)
    end

    def recv(bytes : Slice(UInt8))
      @recv.call(bytes)
    end

    def respond(res : Kafka::Response)
      @respond.call(res)
    end

    def completed(req : Kafka::Request, res : Kafka::Response)
      @completed.call(req, res)
    end

    def failed(req : Kafka::Request, err : Exception)
      @failed.call(req, err)
    end
  end
end
