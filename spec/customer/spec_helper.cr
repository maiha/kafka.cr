require "../spec_helper"

def kafka_broker : Kafka::Broker
  Kafka::Broker.parse(ENV["KAFKA_BROKER"]? || "kafka")
end
