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

  options :api_key, :api_ver, :raw, :hexdump, :verbose, :version, :help

  option max_bytes : Int32, "--max-bytes SIZE", "Read maximum bytes from the file", 4096

  usage <<-EOF
Usage: #{app_name} [options] packet_file

Options:

Example:
  #{PROGRAM_NAME} send.pcap
EOF

  def execute
    Kafka.logger = STDOUT

    paths = args.dup
    base_names = paths.map{|i| File.basename(i)}
    max_head_size = base_names.map(&.size).max
    # max_head_size = paths.size.to_s.size

    paths.each_with_index do |path, i|
      bin = build_binary(path)
      binary_history << bin
      request_found(bin)  if bin.is_a?(Request)
      response_found(bin) if bin.type.response?
      do_show(bin, i, paths.size)
      prev = bin
    end
  end

  protected def do_show(bin : Binary, index : Int32, total : Int32)
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

    # don't show numbering when there is only one file and verbose mode
    if !(verbose && total == 1)
      puts "[#{index+1}] #{body}"
    end

    if verbose
      if klass = bin.klass?
        show_verbose(klass, bin.bytes)
      else
        puts bin.bytes
      end
    end
  end

  private def show_verbose(klass, bytes)
    io = IO::Memory.new(bytes)
    Kafka.debug = true
    res = klass.from_kafka(io)
    # For FetchResponse, access the records to avoid lazy loading
    case res
    when Kafka::Protocol::FetchResponseV6
      res.load!
    end
    Kafka.debug = false

    extra_bytes = Bytes.new(1024)
    if n = io.read_fully?(extra_bytes)
      logger.error "FAIL: extra data found: %s bytes" % [n == 1024 ? "1K+" : n]
    end
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
      # 1st priority
      case path
      when /\b(req|request)\b/i  ; return Type::Request
      when /\b(res|response)\b/i ; return Type::Response
      end

      # 2nd priority
      basename = path.sub(/\.[^.]+$/, "")
      case basename
      when /d$/i ; return Type::Request
      when /s$/i ; return Type::Response
      end

    else
      return last_binary.type
    end

    return Type::Unknown
  end

  private def guess_api_key_and_ver(path : String) : {Kafka::Api?, Int16?}
    # complete api_key by path name
    if api_key == -1
      if guessed = Kafka::Api.guess?(path)
        self.api_key = guessed.value
      end
    end

    # 1st priority: cli argments
    if api_key >= 0 && api_ver >=0
      return {Kafka::Api.from_value(api_key), api_ver.to_i16}
    end

    File.open(path) do |io|
      io.read_bytes(Int32, IO::ByteFormat::BigEndian) # strip first
      # check first Int16
      # a) request  header # api_key => INT16
      # b) response header # correlation_id => INT32
      got_key = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
      got_ver = io.read_bytes(Int16, IO::ByteFormat::BigEndian)
      key = Kafka::Api.from_value?((api_key >= 0) ? api_key : got_key)
      ver = ((api_ver >= 0) ? api_ver : got_ver).to_i16
      return {key, ver}
    end
  end

  private def read_payload(path)
    File.open(path) do |io|
      length = io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      length = max_bytes if length > max_bytes

      bytes = Bytes.new(length)
      io.read_fully(bytes)
      return bytes
    end
  rescue IO::Error
    logger.error "broken data: #{path}"
    bytes = IO::Memory.new(File.read(path)).to_slice
    if bytes.size > 4
      return bytes[4, bytes.size - 4]
    else
      return Bytes.empty
    end
  end

  private def build_binary(path : String) : Binary
    type     = guess_type(path)
    api, ver = guess_api_key_and_ver(path)
    bytes    = read_payload(path)

    case type
    when .unknown?
      return Request.new(bytes, api: api, ver: ver)
    when .request?
      return Request.new(bytes, api: api, ver: ver)
    end

    # Otherwise, this should be a response.
    return Response.new(bytes, request: guess_previous_request?, api: api, ver: ver)
  end
end
