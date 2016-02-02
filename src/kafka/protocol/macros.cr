module Kafka::Protocol::Structure
  macro structure(name, *properties)
    class {{name.id}}
      include Kafka::Protocol::Structure

      getter {{*properties}}

      def initialize({{ *properties.map { |field| "@#{field.id}".id } }})
      end

      def self.from_kafka(io : IO)
        new({{ *properties.map { |field| "#{field.type}.from_kafka(io)".id } }})
      end

      {{yield}}

      def to_kafka(io : IO)
        {% for field in properties %}
           {{field.var}}.to_kafka(io)
        {% end %}
      end

      def to_slice
        buf = MemoryIO.new
        to_kafka(buf)
        buf.to_slice
      end

      def clone
        {{name.id}}.new({{ *properties.map { |field| (field = field.var if field.is_a?(TypeDeclaration)); "@#{field.id}.clone".id } }})
      end

      macro def ==(other : self) : Bool
        {% for ivar in properties %}
          return false unless @{{ivar.var}} == other.@{{ivar.var}}
        {% end %}
          true
      end

      private def errmsg(code : Int16)
        msg = Kafka::Protocol::Errors.from_value?(code) || "???"
        "#{msg} (#{code})"
      end

      private def errmsg(code : Int16, okmsg : String)
        code == 0 ? okmsg : errmsg(code)
      end
    end
  end

  macro request(api, version)
    def initialize(*args)
      super(Int16.new({{api}}), Int16.new({{version}}), *args)
    end

    def to_kafka(io : IO)
      buf = MemoryIO.new
      super(buf)

      value = buf.to_slice
      io.write_bytes(value.bytesize.to_u32, IO::ByteFormat::BigEndian)
      io.write(value.to_slice)
    end
  end

  macro response
    def self.from_kafka(io : IO)
      size = io.read_bytes(Int32, IO::ByteFormat::BigEndian)  # drop checksum
      super(io)
    end

    def self.from_kafka(slice : Slice)
      from_kafka(MemoryIO.new(slice))
    end
  end
end
