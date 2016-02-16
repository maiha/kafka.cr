module Options
  macro option(declare, short_flag, long_flag, description, default)
    var {{declare}}, {{default}}

    def register_option_{{declare.var.id}}(parser)
      {% if short_flag.stringify =~ /[\s=]/ %}
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
  
  macro def new_parser : OptionParser
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
    die(err)
  end

  protected def show_version
    STDERR.puts "#{$0} #{Kafka::Info::VERSION}"
    STDERR.puts "License #{Kafka::Info::LICENSES}"
    STDERR.puts "Written by #{Kafka::Info::AUTHORS} (#{Kafka::Info::HOMEPAGE})"
    exit 0
  end
  
  protected def die(msg)
    STDERR.puts "ERROR: #{msg}\n".colorize(:red) unless msg.to_s.empty?
    STDERR.puts usage
    STDERR.flush
    exit
  end
  
  protected def execute(request)
    bytes = request.to_slice
    connect do |socket|
      spawn do
        socket.write bytes
        socket.flush
        sleep 0
      end
      return request.class.response.from_kafka(socket)
    end
  end

  ######################################################################
  ### concrete options
  
  macro option_verbose
    option verbose : Bool, "-v", "--verbose", "Verbose output", false
  end

  macro option_version
    option version : Bool, "-V", "--version", "Show version information", false
  end

  macro option_help
    option help : Bool, "-h", "--help", "Show this help", false
  end

  macro option_broker
    option broker : String, "-b URL", "--broker=URL", "The connection string for broker", "127.0.0.1:9092"
    private def build_broker
      Kafka::Cluster::Broker.parse(self.broker)
    end

    protected def connect
      broker = build_broker
      socket = TCPSocket.new broker.host, broker.port

      begin
        yield(socket)
      ensure
        socket.close
      end
    end

    protected def execute(request)
      connect do |socket|
        bytes = request.to_slice
        spawn do
          socket.write bytes
          socket.flush
          sleep 0
        end

        recv = Kafka::Protocol.read(socket)

        if verbose
          STDERR.puts "recv: #{recv}"
          STDERR.flush
        end

        fake_io = MemoryIO.new(recv)
        return request.class.response.from_kafka(fake_io, verbose)
      end
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

  macro option_max_bytes
    option max_bytes : Int32, "-M BYTES", "--max_bytes=BYTES", "The max byte size for MessageSet.", 1024
  end
end
