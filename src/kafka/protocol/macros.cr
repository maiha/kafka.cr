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

  macro request(api_key, version)
    def initialize(*args)
      super(Int16.new({{api_key}}), Int16.new({{version}}), *args)
    end

    def to_kafka(io : IO)
      buf = IO::Memory.new
      super(buf)

      value = buf.to_slice
      io.write_bytes(value.bytesize.to_u32, IO::ByteFormat::BigEndian)
      io.write(value.to_slice)
    end
  end

  macro define_from_kafka
    def self.from_kafka(io : IO, debug_level = -1, hint = "")
      on_debug_head_padding
      body = Kafka::Protocol.from_kafka(io)
      super(body, debug_level_succ)
    end

    def self.from_kafka(io : IO, verbose : Bool)
      from_kafka(io, (verbose ? 0 : -1))
    end
  end
end

module Kafka::Protocol
  macro api(key, name, ver = nil)
    {% klass = (ver == nil) ? name : (name.stringify + "V" + ver.stringify).id %}
    # (ver== ): FooRequest
    # (ver==0): FooV0Request
    # (ver==1): FooV1Request
    
    class {{klass}}Response < Structure::{{klass}}Response
      include Kafka::Response
      define_from_kafka
    end

    class {{klass}}Request < Structure::{{klass}}Request
      include Kafka::Request
      request {{key}}, {{ver || 0}}

      def {{klass}}Request.response
        Kafka::Protocol::{{klass}}Response
      end

      define_from_kafka
    end
  end
end
