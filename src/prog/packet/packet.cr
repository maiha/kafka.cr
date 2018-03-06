require "../app"

class Packet < App
  include Options

  options :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] packet_file

Options:

Example:
  #{PROGRAM_NAME} send.pcap
EOF

  def do_read(path : String)
    io = IO::Memory.new(File.read(path))
    io = Kafka::Protocol.from_kafka(io)
    puts io.to_slice
  end

  def execute
    args.each do |path|
      do_read(path)
    end
  end
end
