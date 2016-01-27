require "./request"

module Kafka::Protocol
  class HeartbeatRequest < Request::Base
    #                           generation_id ---+
    # +-----------+------+-----+-----------+-----|-----+-----------+
    # |    size   |  key | api |SIZE |g_id |     o     |SIZE| m_id |
    # +-----------+------+-----+-----------+-----------+-----------+
    #      unt32    unt16 unt16 unt16 bytes  unt32      unt16 bytes

    request 12 do
      field group_id : String, "x"
      field generation_id : Int32, -1
      field member_id : String, "cr"
    end
  end

  class HeartbeatResponse
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
