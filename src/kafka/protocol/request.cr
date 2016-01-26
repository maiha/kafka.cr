module Kafka::Protocol
  class Request
    macro fields(properties)
      include Format

      {% for key, value in properties %}
        {% if key == :key || key == :ver %}
          {% properties[key] = {type: Int16, default: value} %}
        {% else %}
          {% properties[key] = {type: value} unless value.is_a?(HashLiteral) %}
        {% end %}
      {% end %}
      {% full_args = properties.keys.map(&.id) %}
      {% minimum_args = properties.to_a.reject { |a| a[1][:default] }.map(&.[0].id) %}

      def initialize({{ *full_args }})
        {% for arg in minimum_args %}
          self.{{ arg }} = {{ arg }}
        {% end %}
      end

      def initialize({{ *minimum_args }})
        {% for arg in minimum_args %}
          self.{{ arg }} = {{ arg }}
        {% end %}
      end

      {% for key, value in properties %}
        {% properties[key] = {type: value} unless value.is_a?(HashLiteral) %}

        def {{key.id}}=(_{{key.id}} : {{value[:type]}})
          @{{key.id}} = _{{key.id}}
          self
        end

        def default_{{key.id}} : {{value[:type]}}
          {% if value[:default] && value[:type].id.symbolize == :Int16 %}
            {{value[:default]}}.to_i16
          {% elsif value[:default] %}
            {{value[:default]}}
          {% else %}
            raise "{{@type.name}}#{{key.id}} is not set"
          {% end %}
        end

        def {{key.id}} : {{value[:type]}}
          if @{{key.id}}.nil?
            default_{{key.id}}
          else
            @{{key.id}}.not_nil!
          end
        end
      {% end %}

      def to_payload(io : IO)
        {% for key, value in properties %}
          write(io, {{key.id}})
        {% end %}
      end

      def to_payload : Slice
        io = MemoryIO.new
        to_payload(io)
        io.to_slice
      end

      def to_binary(io : IO) : Slice
        body = to_payload
        head = body.size.to_u32
        write(io, head)
        write(io, body)
      end

      def to_binary : Slice
        io = MemoryIO.new
        to_binary(io)
        io.to_slice
      end
    end
  end
end
