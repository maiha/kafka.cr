module Kafka::Protocol
  class Request
    include Format

    macro request(no)
      # +--------+--------+----------------+-------------------+
      # | key    | api    | correlation    |(head)  | client   |
      # +--------+--------+----------------+-------------------+
      #  unt16    unt16    unt32            unt16    bytes

      field api_key        : Int16 , {{no.id}}_i16  # request no
      field api_version    : Int16 , 0_i16          # current api version is 0
      field correlation_id : Int32 , 1
      field client_id      : String, "cr"

      {{ yield }}

      # binary for kafka protocol
      def to_binary(io : IO) : Slice
        body = to_slice
        head = body.size.to_u32
        write(io, head)
        write(io, body)
      end

      # binary for kafka protocol
      def to_binary : Slice
        io = MemoryIO.new
        to_binary(io)
        io.to_slice
      end

      def to_slice : Slice
        io = MemoryIO.new
        to_slice(io)
        io.to_slice
      end
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
      def to_slice(io : IO)
        {% for m in @type.methods.map(&.name).select{|s| s =~ /^write_field_/} %}
          {{ m.id }}(io)
        {% end %}
      end
    end
  end
end
