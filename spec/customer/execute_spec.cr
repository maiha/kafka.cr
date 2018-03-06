require "./spec_helper"

describe "(customer: execution)" do
  subject!(kafka) { Kafka.new(kafka_broker) }
  after { kafka.close }

  let(request) { Kafka::Protocol::HeartbeatRequest.new(0, "x", "y", -1, "cr") }

  describe "(with good handler)" do
    let(handler) { Kafka::Handlers::Tracing.new }

    it "invoke handlers in order" do
      kafka.execute(request, handler)

      expect(handler.traces).to eq ["request", "send", "recv", "respond", "completed"]
    end
  end

  describe "(with broken handler)" do
    class BadImplementedError < Exception; end

    class BrokenHandler < Kafka::Handlers::Tracing
      def verbose : Bool
        raise BadImplementedError.new("emmulates bad implemented handler")
      end
    end

    let(handler) { BrokenHandler.new }

    it "invoke failed handler" do
      expect {
        kafka.execute(request, handler)
      }.to raise_error(BadImplementedError)

      expect(handler).to_be.invoked?("failed")
    end
  end
end
