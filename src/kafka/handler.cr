class Kafka
  module Handler
    abstract def verbose : Bool
    abstract def request(req : Kafka::Request)
    abstract def send(bytes : Slice(UInt8))
    abstract def recv(bytes : Slice(UInt8))
    abstract def respond(res : Kafka::Response)
    abstract def completed(req : Kafka::Request, res : Kafka::Response)
    abstract def failed(req : Kafka::Request, err : Exception)
  end
end

require "./handlers/**"
