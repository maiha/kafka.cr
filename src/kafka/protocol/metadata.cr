require "./request"

module Kafka::Protocol
  class MetadataRequest < Request::Base
    request 3 do
      field topics : Array(String), [] of String
    end
  end

  class MetadataResponse
    getter correlation_id, error_code

    class Broker
      getter :id, :host, :port
      def initialize(@id : Int32, @host : String, @port : Int32)
      end
    end

    def initialize(
          @correlation_id : Int32,
          @error_code : Int16,
          @brokers : Array(Broker)
        )
    end

    def self.from_io(io : IO)
      len = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      # len should == 6
      cid = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      err = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
      new(cid, err, [] of Broker)
    end
  end
end
