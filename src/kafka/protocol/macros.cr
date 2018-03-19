module Kafka::Protocol::Structure
  macro structure(name, *properties)
    class {{name.id}}
      include Kafka::Protocol::Structure

      getter {{*properties}}

      def initialize({{ *properties.map { |field| "@#{field.id}".id } }})
      end

      def self.from_kafka(io : IO, debug_level = nil, hint = "")
        debug_level ||= Kafka.logger_debug_level_default
        debug kafka_label, color: :yellow
        new({{ *properties.map { |field| "#{field.type}.from_kafka(io, debug_level_succ, :#{field.var})".id } }})
      end

      def self.from_kafka(slice : Slice, debug_level = nil, hint = "")
        debug_level ||= Kafka.logger_debug_level_default
        from_kafka(IO::Memory.new(slice), debug_level, hint)
      end

      {{yield}}

      def to_kafka(io : IO)
        {% for field in properties %}
          {{field.var}}.to_kafka(io)
        {% end %}
      end

      def to_slice
        buf = IO::Memory.new
        to_kafka(buf)
        buf.to_slice
      end

      def clone
        {{name.id}}.new({{ *properties.map { |field| (field = field.var if field.is_a?(TypeDeclaration)); "@#{field.id}.clone".id } }})
      end

      def ==(other : self) : Bool
        {% for ivar in properties %}
          return false unless @{{ivar.var}} == other.@{{ivar.var}}
        {% end %}
          true
      end

      # expects error_code
      def errmsg
        Kafka::Protocol.errmsg(error_code)
      end

      def error?
        error_code != 0
      end
    end
  end
end

module Kafka::Protocol
  @@requests = Hash(Tuple(Int16, Int16), Kafka::Request.class).new
  def self.requests
    @@requests
  end

  def self.request?(api : Int, ver : Int) # Kafka::Request.class?
    requests[{api, ver}]?
  end

  def self.request(api : Int, ver : Int) : Kafka::Request.class
    request?(api, ver) || raise "No request class defined for (api:#{api}, ver:#{ver})"
  end

  macro protocol(name, ver = nil)
    {% v = (ver == nil) ? "".id : ("V" + ver.stringify).id %}
    # (ver== ): FooRequest
    # (ver==0): FooRequestV0
    # (ver==1): FooRequestV1

    class {{name}}Request{{v}} < Kafka::Request
      API = Kafka::Api::{{name}}
      VER = Int16.new({{ver}} || 0)

      Kafka::Protocol.requests[{API.value.to_i16, VER}] = {{name}}Request{{v}}.as(Kafka::Request.class)

      forward_missing_to @request
      
      def self.from_kafka(io : IO, debug_level = nil, hint = "")
        debug_level ||= Kafka.logger_debug_level_default
        request = Structure::{{name}}Request{{v}}.from_kafka(io, debug_level_succ)
        new(request)
      end

      def self.new(*args)
        request = Structure::{{name}}Request{{v}}.new(API.value.to_i16, VER, *args)
        new(request)
      end

      def initialize(@request : Structure::{{name}}Request{{v}})
      end

      def bytes
        @request.to_slice
      end

      def to_kafka(io : IO)
        @request.to_kafka(io)
      end

      def {{name}}Request{{v}}.response
        Kafka::Protocol::{{name}}Response{{v}}
      end
    end

    class {{name}}Response{{v}} < Structure::{{name}}Response{{v}}
      include Kafka::Response
    end
  end
end
