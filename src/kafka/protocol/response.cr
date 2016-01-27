module Kafka::Protocol
  class Response
    include Format

    # write kafka protocol binary into io
    def to_io(io : IO)
      body = to_bytes
      head = body.size.to_u32
      write(io, head)
      write(io, body)
    end

    # binary for kafka protocol that consists of header and bytes
    def to_binary : Slice
      io = MemoryIO.new
      to_io(io)
      io.to_slice
    end

    def to_bytes : Slice
      io = MemoryIO.new
      to_bytes(io)
      io.to_slice
    end

    macro response
      # +--------+------------------------+
      # | size   | payload                |
      # +--------+------------------------+
      #  unt32    bytes(size)

      field size           : Int32

      {{ yield }}

    end

    macro field(declare, default)
      {% var = declare.var %}
      {% type = declare.type %}

      def {{var.id}}=(_{{var.id}} : {{type}})
        @{{var.id}} = _{{var.id}}
        self
      end

      def {{var.id}}! : {{type}}
        if @{{var.id}}.nil?
          {% if default %}
            {{default}}
          {% else %}
            raise "{{@type.name}}#{{var.id}} is not set"
          {% end %}
        else
          @{{var.id}}.not_nil!
        end
      end

      private def write_field_{{var.id}}(io : IO)
        write(io, {{var.id}}!)
      end
    end

    macro field(prop)
      field({{prop}}, nil)
    end

    macro response(klass)
      def to_bytes(io : IO)
        {% for m in @type.methods.map(&.name).select { |s| s =~ /^write_field_/ } %}
          {{ m.id }}(io)
        {% end %}
      end
    end
  end
end
