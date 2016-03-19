require "./spec_helper"

describe Kafka::Connection do
  subject!(kafka) { Kafka.new }
  after { kafka.close }

  it "open and close" do
    kafka.close
  end

  it "#socket! open a connection lazily" do
    expect(kafka.connected?).to eq(false)
    kafka.socket!
    expect(kafka.connected?).to eq(true)
  end
end
