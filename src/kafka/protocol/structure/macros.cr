module Kafka::Protocol::Structure
  macro structure(name, *properties)
    class {{name.id}}
      include Kafka::Protocol::Structure
      include Kafka::Protocol::Format

      getter {{*properties}}

      def self.from_io(io : IO)
        new(
          {% cnt = 0 %}
          {% for field in properties %}
            Kafka::Protocol::Format::FromIO.from_io(io, {{field.type}})
            {% if (cnt += 1) < properties.size %}
              ,
            {% end %}
          {% end %}
        )
      end

      {% if name.id.stringify =~ /heartbeatres/i  %}
#x
      {% end %}
      def initialize({{ *properties.map { |field| "@#{field.id}".id } }})
      end

      {{yield}}

      def to_io(io : IO)
        {% for field in properties %}
           to_io(io, {{field.var}})
        {% end %}
      end

      def to_slice
        buf = MemoryIO.new
        to_io(buf)
        buf.to_slice
      end

      def clone
        {{name.id}}.new({{ *properties.map { |field| (field = field.var if field.is_a?(TypeDeclaration)); "@#{field.id}.clone".id } }})
      end
    end
  end

  macro request(api, version)
    def initialize(*args)
      super(Int16.new({{api}}), Int16.new({{version}}), *args)
    end

    def to_io(io : IO)
      buf = MemoryIO.new
      super(buf)

      value = buf.to_slice
      io.write_bytes(value.bytesize.to_u32, IO::ByteFormat::BigEndian)
      io.write(value.to_slice)
    end
  end

  macro response
    def self.from_io(io : IO)
      size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)  # drop checksum
      super(io)
    end

    def self.from_io(slice : Slice)
      from_io(MemoryIO.new(slice))
    end
  end
end
