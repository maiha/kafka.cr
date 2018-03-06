module Kafka::Protocol::Structure
  macro structure(name, *properties)
    class {{name.id}}
      include Kafka::Protocol::Structure

      getter {{*properties}}

      def initialize({{ *properties.map { |field| "@#{field.id}".id } }})
      end

      def self.from_kafka(io : IO, debug_level = nil, hint = "")
        debug_level ||= Kafka.logger_debug_level_default
        on_debug_head_padding
        label = self.to_s.sub(/Kafka::Protocol::Structure::/, "")
        on_debug "(#{label}.from_kafka)"
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

  def self.request(api : Int, ver : Int) : Kafka::Request.class
    requests[{api, ver}]? || raise "No request class defined for (api:#{api}, ver:#{ver})"
  end

  module FromKafka
    macro included
      def self.from_kafka(io : IO, debug_level = -1, hint = "")
        on_debug_head_padding
        super(io, debug_level_succ)
      end

      def self.from_kafka(io : IO, verbose : Bool)
        from_kafka(io, (verbose ? 0 : -1))
      end
    end
  end

  macro protocol(name, ver = nil)
    {% klass = (ver == nil) ? name : (name.stringify + "V" + ver.stringify).id %}
    # (ver== ): FooRequest
    # (ver==0): FooV0Request
    # (ver==1): FooV1Request

    class {{klass}}Request < Kafka::Request
      API = Kafka::Api::{{name}}
      VER = Int16.new({{ver}} || 0)

      Kafka::Protocol.requests[{API.value.to_i16, VER}] = {{klass}}Request.as(Kafka::Request.class)

      forward_missing_to @request
      
      def initialize(*args)
        @request = Structure::{{klass}}Request.new(API.value.to_i16, VER, *args)
      end

      def bytes
        @request.to_slice
      end

      def to_kafka(io : IO)
        @request.to_kafka(io)
      end

      def {{klass}}Request.response
        Kafka::Protocol::{{klass}}Response
      end

      include Kafka::Protocol::FromKafka
    end

    class {{klass}}Response < Structure::{{klass}}Response
      include Kafka::Response
      include Kafka::Protocol::FromKafka
    end
  end
end
