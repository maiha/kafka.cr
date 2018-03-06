  enum Type
    Request
    Response
    Unknown
  end

  module Binary
    abstract def type : Type
    abstract def bytes : Bytes
    abstract def implemented? : Bool

    def valid? : Bool
      if klass = klass?
        io = IO::Memory.new(bytes)
        klass.from_kafka(io)

        extra_bytes = Bytes.new(1)
        if io.read_fully?(extra_bytes)
          return false
        else
          return true
        end
      else
        return false
      end
    end
  end

  class Request
    include Binary
    getter bytes

    def initialize(@bytes : Bytes, @api : Kafka::Api?, @ver : Int16)
    end

    def type
      Type::Request
    end

    def klass?
      if (api = @api) && (klass = Kafka::Protocol.request?(api.value, @ver))
        return klass
      end
      return nil
    end

    def implemented? : Bool
      !! klass?
    end

    def to_s(io : IO)
      if api = @api
        io << "%s Request" % [api]
        io << " (Version: %d)" % [@ver] if @ver > 0
      else
        io << "(Unknown Request)"
      end
    end
  end

  class Response
    include Binary
    getter bytes

    def initialize(@bytes : Bytes, @request : Request?)
    end

    def type
      Type::Response
    end

    def klass?
      @request.try(&.klass?.try(&.response))
    end

    def implemented? : Bool
      !! klass?
    end

    def to_s(io : IO)
      if req = @request
        io << req.to_s.sub(/Request/, "Response")
      else
        io << "(Unknown Response)"
      end
    end
  end

  class Unknown
    include Binary
    getter bytes

    def initialize(@bytes : Bytes)
    end

    def type
      Type::Unknown
    end

    def implemented? : Bool
      false
    end

    def valid? : Bool
      false
    end

    def to_s(io : IO)
      io << "Unknown Binary"
    end
  end

