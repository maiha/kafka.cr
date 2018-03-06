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
  def execute
    args.each do |path|
      do_read(path)
    end
  end

  protected def do_read(path : String)
    io = IO::Memory.new(File.read(path))
    io = Kafka::Protocol.from_kafka(io)

    # check first Int16
    # a) request  header # api_key => INT16
    # b) response header # correlation_id => INT32
    api = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
    ver = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
    io.rewind

    if api = Kafka::Api.from_value?(api)
      # This packet may be a request.
      do_show_request(api, ver, io)
    else
      # This packet may be a response.
      do_show_response(io)
    end
  end

  private def do_show_request(api, ver, io)
    puts Kafka::Protocol.request(api.value, ver).to_s
  end

  private def do_show_response(io)
    puts "guessed response"
  end
end
