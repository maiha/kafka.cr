require "./spec_helper"

include Kafka::Protocol

describe HeartbeatRequest do
  it "create request" do
    bin = HeartbeatRequest.new(
      group_id = "x",
      generation_id = 0,
      member_id = "y"
    ).to_binary
    #                                         generation_id ---+
    #               +-----------+------+-----+-----------+-----|-----+-----------+
    #               |    size   |  key | api |SIZE |g_id |     o     |SIZE| m_id |
    #               +-----------+------+-----+-----------+-----------+-----------+
    #                    unt32    unt16 unt16 unt16 bytes  unt32      unt16 bytes
    bin.should eq(u8(0, 0, 0, 14, 0, 12, 0, 0, 0, 1, 120, 0, 0, 0, 0, 0, 1, 121))
  end
end
