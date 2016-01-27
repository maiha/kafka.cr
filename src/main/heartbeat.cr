require "option_parser"
require "socket"
require "../kafka"
require "./utils/*"

dump = false
opts = OptionParser.parse(ARGV) do |parser|
  parser.on("-d", "--dump", "Dump octal raw data to stdout") { dump = true }
end

broker = Kafka::Cluster::Broker.parse(ARGV.shift.not_nil!)
socket = TCPSocket.new broker.host, broker.port

req = Kafka::Protocol::HeartbeatRequest.new
bytes = req.to_binary

spawn do
  socket.write bytes
  socket.flush
  sleep 0
end

if dump
  p Kafka::Protocol.read(socket)
else
  p Kafka::Protocol::HeartbeatResponse.from_io(socket)
end
