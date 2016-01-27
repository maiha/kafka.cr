require "./request"

module Kafka::Protocol
  class MetadataRequest < Request
    request 3 do
      field topics : Array(String)
    end

    response MetadataResponse
  end

  class MetadataResponse
    getter correlation_id, error_code

    def initialize(@correlation_id : Int32, @error_code : Int16)
    end

    def self.from_io(io : IO)
      len = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      # len should == 6
      cid = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      err = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
      new(cid, err)
    end
  end
end
