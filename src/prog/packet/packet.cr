require "../app"
require "./data"

private def truncate_bytes(bytes : Bytes, limit : Int32)
  if bytes.size > limit
    bytes[0, limit].to_s.sub(/\]$/, "") + " ..."
  else
    bytes
  end
end

class Packet < App
  include Options

  property guess_type_by_filename : Bool = true
  property guess_api_by_sequence  : Bool = true

  property request_queue : Array(Request) = Array(Request).new
  property binary_history : Array(Binary) = Array(Binary).new

  options :raw, :verbose, :version, :help

  usage <<-EOF
Usage: #{app_name} [options] packet_file

Options:

Example:
  #{PROGRAM_NAME} send.pcap
EOF

  def execute
    paths = args.dup
    base_names = paths.map{|i| File.basename(i)}
    max_head_size = base_names.map(&.size).max
    # max_head_size = paths.size.to_s.size

    paths.each_with_index do |path, i|
      bin = build_binary(path)
      binary_history << bin
      request_found(bin)  if bin.is_a?(Request)
      response_found(bin) if bin.type.response?
      # head = "%-#{max_head_size}s:" % base_names[i]
      head = "[#{i+1}]"
      do_show(bin, head: head)
      prev = bin
    end
  end

  protected def do_show(bin : Binary, head : String)
    if raw
      if verbose
        body = bin.bytes.to_s
      else
        body = truncate_bytes(bin.bytes, limit: 15).to_s
      end
    else
      body = bin.to_s
    end

    if bin.implemented?
      if bin.valid?
        body = body.colorize.green
      else
        body = body.colorize.red
      end
    else
      body = body.colorize.yellow
    end
    puts "#{head} #{body}"

    if verbose
      if klass = bin.klass?
        if bin.valid?
          show_verbose(klass, bin.bytes)
        else
          # invalid data
          puts "FAILED: #{klass}.from_kafka(#{bin.bytes})".colorize(:red)
        end
      else
        puts bin.bytes
      end
    end
  end

  private def show_verbose(klass, bytes)
    io = IO::Memory.new(bytes)
    Kafka.debug = true
    klass.from_kafka(io)
    Kafka.debug = false
  end

  private def request_found(req : Request)
    request_queue << req
  end

  private def response_found(bin)
    request_queue.shift?
  end

  private def guess_previous_request? : Request?
    if guess_api_by_sequence && request_queue.any?
      return request_queue.first
    end
    return nil
  end

  private def last_binary
    binary_history.last? ||
      ::Unknown.new(bytes: Kafka::Protocol::Structure::Null)
  end

  private def guess_type(path : String) : Type
    if guess_type_by_filename
      basename = path.sub(/\.[^.]+$/, "")
      # 1st priority
      case basename
      when /request/i  ; return Type::Request
      when /response/i ; return Type::Response
      end

      # 2nd priority
      case basename
      when /d$/i ; return Type::Request
      when /s$/i ; return Type::Response
      end

    else
      return last_binary.type
    end

    return Type::Unknown
  end

  private def build_binary(path : String) : Binary
    type = guess_type(path)
    
    io = IO::Memory.new(File.read(path))
    io = Kafka::Protocol.from_kafka(io)
    bytes = io.to_slice

    # check first Int16
    # a) request  header # api_key => INT16
    # b) response header # correlation_id => INT32
    api = Kafka::Api.from_value?(io.read_bytes(Int16, IO::ByteFormat::BigEndian))
    ver = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
    io.rewind

    case type
    when .unknown?
      # do best effort by checking bytes
      if api && (ver <= 5)
        # When request parser can parse this, confirm it is a request.
        return Request.new(bytes, api: api, ver: ver)
      end
    when .request?
      return Request.new(bytes, api: api, ver: ver)
    end

    # Otherwise, this should be a response.
    return Response.new(bytes, request: guess_previous_request?)
  end
end
