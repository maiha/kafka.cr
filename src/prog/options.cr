module Options
  class OptionError < Exception
  end
  
  macro option(declare, long_flag, description, default)
    var {{declare}}, {{default}}

    def register_option_{{declare.var.id}}(parser)
      {% if long_flag.stringify =~ /[\s=]/ %}
        {% if declare.type.stringify == "Int64" %}
          parser.on({{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i64}
        {% elsif declare.type.stringify == "Int32" %}
          parser.on({{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i32}
        {% elsif declare.type.stringify == "Int16" %}
          parser.on({{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i16}
        {% else %}
          parser.on({{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x}
        {% end %}
      {% else %}
        parser.on({{long_flag}}, {{description}}) {self.{{declare.var}} = true}
      {% end %}
    end
  end

  macro option(declare, short_flag, long_flag, description, default)
    var {{declare}}, {{default}}

    def register_option_{{declare.var.id}}(parser)
      {% if long_flag.stringify =~ /[\s=]/ %}
        {% if declare.type.stringify == "Int64" %}
          parser.on({{short_flag}}, {{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i64}
        {% elsif declare.type.stringify == "Int32" %}
          parser.on({{short_flag}}, {{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i32}
        {% elsif declare.type.stringify == "Int16" %}
          parser.on({{short_flag}}, {{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x.to_i16}
        {% else %}
          parser.on({{short_flag}}, {{long_flag}}, {{description}}) {|x| self.{{declare.var}} = x}
        {% end %}
      {% else %}
        parser.on({{short_flag}}, {{long_flag}}, {{description}}) {self.{{declare.var}} = true}
      {% end %}
    end
  end

  macro options(*names)
    {% for name in names %}
      option_{{name.id.stringify.id}}
    {% end %}
  end

  def new_parser : OptionParser
    OptionParser.new.tap{|p|
      {% for name in @type.methods.map(&.name.stringify) %}
        {% if name =~ /\Aregister_option_/ %}
          {{name.id}}(p)
        {% end %}
      {% end %}
    }
  end

  macro usage(str)
    def usage
      {{str}}.sub(/^(Options:.*?)$/m){ "#{$1}\n#{new_parser}" }
    end
  end

  @parser : OptionParser?
  val parser = new_parser
  protected def parse_args!
    parser.parse(args)
    if help
      die("")
    end
    if version
      show_version
    end
  rescue err
    raise OptionError.new("#{err}")
  end

  # #####################################################################
  # ## concrete options
  macro option_all(default = false)
    option all : Bool, "-a", "--all", "Process all data", {{default}}
  end

  macro option_api_key
    option api_key : Int32, "--api-key API_KEY", "Specify the number of api key", -1
  end

  macro option_api_ver
    option api_ver : Int32, "--api-ver API_VERSION", "Specify api version of the request", -1
  end

  macro option_hexdump
    def register_option_hexdump(parser)
      parser.on("-X", "--hexdump", "Use hexdump for address (w/ full packet)") {
        Kafka.logger_hexdump = true
      }
    end
  end
  
  macro option_count
    option count : Bool, "-c", "--count", "Print a count of entries", false
  end

  macro option_consumer_offsets
    option consumer_offsets : Bool, "-c", "--consumer_offsets", "Show consumer offsets", false
  end

  macro option_dump
    option dump : Bool, "-d", "--dump", "Dump octal data (Simulation mode)", false
  end

  macro option_guess
    option guess : Bool, "-g", "--guess", "Use guess mode", false
  end

  macro option_nop
    option nop : Bool, "-n", "--nop", "Show request data (Simulation mode)", false
  end

  macro option_verbose
    option verbose : Bool, "-v", "--verbose", "Verbose output", false
  end

  macro option_verbose2
    option verbose2 : Bool, "-vv", "Increase the verbosity", false
  end

  macro option_version
    option version : Bool, "-V", "--version", "Show version information", false
  end

  macro option_help
    option help : Bool, "-h", "--help", "Show this help", false
  end

  macro option_json
    option json : Bool, "-j", "--json", "Use json output", false
  end

  macro option_partition
    option partition : Int32, "-p NUM", "--partition NUM", "Specify partition number", 0
  end

  macro option_correlation_id
    option correlation_id : Int32, "--correlation_id VALUE", "A user-supplied integer value that will be passed back with the response", 0
  end

  macro option_broker
    option broker : String, "-b URL", "--broker=URL", "The connection string for broker", "127.0.0.1:9092"
    private def build_broker
      Kafka::Broker.parse(self.broker)
    end

    protected def connect(broker = build_broker)
      socket = TCPSocket.new broker.host, broker.port

      begin
        yield(socket)
      ensure
        socket.close
      end
    end

    protected def execute(request, broker = build_broker)
      Kafka.debug = true if verbose
      kafka = Kafka.new(broker)
      kafka.execute(request)
    end

    protected def fetch_topic_metadata(topics, app_name)
      req = Kafka::Protocol::MetadataRequestV0.new(0, app_name, topics)
      return execute(req)
    end
  end

  macro option_topic
    option topic : String, "-t TOPIC", "--topic=TOPIC", "The topic to get metadata", ""
  end

  macro option_wait
    option wait : Int32, "-w MSEC", "--wait=MSEC", "The max wait msec to block waiting data.", 1000
  end

  macro option_offset
    option offset : Int64, "-o OFFSET", "--offset=OFFSET", "The offset to get from.", 0_i64
  end

  macro option_raw
    option raw : Bool, "--raw", "Show raw data for output.", false
  end

  macro option_max_bytes
    option max_bytes : Int32, "-M BYTES", "--max_bytes=BYTES", "The max byte size for MessageSet.", 1024
  end
end
