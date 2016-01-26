require "../kafka"

include Kafka::Protocol

req = HeartbeatRequest.new
bytes = req.to_binary

# STDOUT.write(bytes)

require "socket"

socket = TCPSocket.new "127.0.0.1", 9092
socket.write bytes
socket.flush

#socket.read_fully(res_size)
res = HeartbeatResponse.from_io(socket)
p res

