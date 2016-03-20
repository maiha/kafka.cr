require "./spec_helper"

describe Kafka::Protocol::Structure::MessageSet do
  describe "(A length of -1 indicates null)" do
    it "from_kafka" do
      # 255, 255, 255, 255, 0, 0, 0, 0

      #      ms = Kafka::Protocol::Structure::MessageSet.from_kafka(bytes(255, 255, 255, 255, 0, 0, 0, 0))

    end
  end
end
