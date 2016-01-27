require "../kafka"
require "socket"

include Kafka::Protocol

if ARGV.size != 2
  prog = $0  # File.basename(__FILE__, ".cr")
  puts <<-USAGE
    usage: #{prog} host port
           #{prog} 127.0.0.1 9092
    USAGE
  exit
end

host = ARGV.shift.not_nil!
port = ARGV.shift.not_nil!.to_i

socket = TCPSocket.new host, port

req = HeartbeatRequest.new
bytes = req.to_binary

spawn do
  socket.write bytes
  socket.flush
  sleep 0
end

res = HeartbeatResponse.from_io(socket)
p res
