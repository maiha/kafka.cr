class Kafka
  module Handler
    abstract def verbose : Bool
    abstract def request(req : Kafka::Protocol::Request)
    abstract def send(bytes : Slice(UInt8))
    abstract def recv(bytes : Slice(UInt8))
    abstract def respond(res : Kafka::Protocol::Response)
    abstract def completed(req : Kafka::Protocol::Request, res : Kafka::Protocol::Response)
    abstract def failed(req : Kafka::Protocol::Request, err : Exception)
  end
end

require "./handlers/**"
