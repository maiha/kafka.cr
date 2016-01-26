require "../kafka"

include Kafka::Protocol

slice = HeartbeatRequest.new(
      group_id = "x",
      generation_id = -1,
      member_id = "y"
    ).to_binary

#STDOUT.write(slice)

require "socket"

socket = TCPSocket.new "127.0.0.1", 9092
socket.write slice
socket.flush

sleep 3
p socket.read_byte
