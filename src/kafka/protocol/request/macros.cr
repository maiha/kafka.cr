module Kafka::Protocol::Request::Macros
  macro field(declare, default)
    {% var = declare.var %}
    {% type = declare.type %}

    {% if type.id.stringify =~ /\AInt(8|16)\Z/ %}
      def {{var}}=(_{{var}} : Numeric)
        @{{var}} = {{type}}.new(_{{var}})
        self
      end
    {% else %}
      def {{var}}=(_{{var}} : {{type}})
        @{{var}} = _{{var}}
        self
      end
    {% end %}

    def {{var}}! : {{type}}
      if @{{var}}.nil?
        {% if default %}
          {% if type.id.stringify =~ /\AInt(8|16)\Z/ %}
             {{type}}.new({{default}})
          {% else %}
             {{default}}
          {% end %}
        {% else %}
        raise "{{@type.name}}#{{var}} is not set"
        {% end %}
      else
        @{{var}}.not_nil!
      end
    end

    private def write_field_{{var}}(io : IO)
      write(io, {{var}}!)
    end
  end

  macro field(prop)
    field({{prop}}, nil)
  end
end
