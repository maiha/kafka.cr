require "colorize"

macro debug_level_succ
  ((debug_level == -1) ? -1 : debug_level + 1)
end

macro debug_addr(v)
  if Kafka.logger_hexdump
    offset = 4          # kafka packet margin for request | response
    "%x" % ({{v}} + offset)
  else
    {{v}}
  end
end

macro debug_address(base = nil, abs = nil)
  if debug_level >= 0
    {% if abs %}
      _addr_ = {{abs}}
    {% elsif base %}
      _addr_ = {{base}} + io.pos
    {% else %}
      _addr_ = io.pos
    {% end %}
    if Kafka.logger_hexdump
      offset = 4          # kafka packet margin for request | response
      "(%07x)" % (_addr_ + offset)
    else
      "(%07d)" % (_addr_)
    end
  else
    ""
  end
end

macro kafka_label
  _label = self.to_s.sub(/^.*::/, "")
  _label = _label.sub(/(Request|Response)(V(\d+))?$/){
    "#{$1} (Version: #{$~[3]? || 0})"
  }
end

macro debug(msg, color = nil, prefix = nil)
  if debug_level >= 0
    buf = String.build do |_io_|
      case {{prefix || "nil".id}}
      when Int32
        _io_.print debug_address(abs: {{prefix}}.as(Int32))
      when String
        _io_.print {{prefix}}
      else
        _io_.print " " * 9
      end
      _io_.print " " * (debug_level * 2)
      _msg_ = {{msg}}
      {% if color %}
        _msg_ = _msg_.colorize({{color}})
      {% end %}
      _io_.print _msg_
    end
    Kafka.logger.debug buf
  end
end

macro debug2(msg, group = nil)
  if Kafka.debug2?
    {% if group %}
      Kafka.logger.debug "debug2: " + {{group}}.to_s + ": " + {{msg}}.to_s
    {% else %}
      Kafka.logger.debug "debug2: " + {{@type.name.stringify.gsub(/^.*::/,"")}} + ": " + {{msg}}.to_s
    {% end %}
  end
end

macro read_primitive(klass, color, prefix = nil)
  name = hint.to_s.empty? ? "" : "(#{hint})"
  bytes = {
    "Int64"  => 8,
    "Int32"  => 4,
    "UInt32" => 4,
    "Int16"  => 2,
    "Int8"   => 1,
    "Bool"   => 1,
  }[{{klass.id}}.to_s] || "?"
  label = {{klass.id}}.to_s + "[#{bytes}]#{name}"
  begin
    {% if klass.id.stringify == "Bool" %}
      value = io.read_bytes(Int8, IO::ByteFormat::BigEndian)
    {% else %}
      value = io.read_bytes({{klass.id}}, IO::ByteFormat::BigEndian) # {{klass.id}}
    {% end %}
    if hint.to_s == "error_code" && value != 0
      errmsg = Kafka::Protocol.errmsg(value.to_i16)
      debug "#{label} -> #{errmsg}", color: :red, prefix: {{prefix}}
    else
      debug "#{label} -> #{value}", color: {{color}}, prefix: {{prefix}}
    end

    {% if klass.stringify == "Bool" %}
      return (value == 1)
    {% else %}
      return value
    {% end %}
  rescue err
    debug "#{label} (#{err})", color: :red, prefix: {{prefix}}
    raise err
  end
end
