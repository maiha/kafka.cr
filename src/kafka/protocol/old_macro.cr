require "./format"

module Kafka::Protocol
  class OldRequest
    FIELD_NAMES = [] of String
    WRITE_PROCS = [] of Proc(String)

    include Format

    macro field(prop)
      property! {{prop}}
      FIELD_NAMES << {{prop.var.stringify}}
      WRITE_PROCS << ->(v : {{prop.type}}) { write({{prop.var}}) }
    end

    def to_binary : String
      fields.inspect
    end

    def fields : Array(String)
      FIELD_NAMES
    end

    macro def values : Array(Object)
      {{@type = uninitialized FIELD_NAMES}}
    end
  end

  macro request(name, api_key, api_version)
    class {{name}} < Request
      field api_key : Int16
      field api_version : Int16

      def api_key
        @api_key || {{api_key}}
      end

      def api_version
        @api_version || {{api_version}}
      end

      {{yield}}
    end
  end
end

#  request HeartbeatRequest, 12, 0 do
#    field correlation_id : String
#  end

#  class HeartbeatRequest < Request
#    field api_key : Int16
#    field api_version : Int16
#    field correlation_id : String
#  end
