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

macro debug_head_padding
  if debug_level >= 0
    Kafka.logger_debug_prefix = " " * 9
  else
    Kafka.logger_debug_prefix = ""
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

macro debu_set_head_address(base = nil, abs = nil)
  Kafka.logger_debug_prefix = debug_address({{base}}, {{abs}})
end

macro debug_label
  _label = self.to_s.sub(/^.*::/, "")
  _label = _label.sub(/(Request|Response)(V(\d+))?$/){
    "#{$1} (Version: #{$~[3]? || 0})"
  }
  debug _label
end

macro debug(msg, color = nil, prefix = nil)
  if debug_level >= 0
    buf = String.build do |_io_|
      {% if prefix %}
        _io_.print {{prefix}}
      {% else %}
        _io_.print Kafka.logger_debug_prefix
      {% end %}
      _io_.print " " * (debug_level * 2)
      _msg_ = {{msg.id}}.to_s
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

macro io_read_bytes_with_debug(color, type, prefix = nil)
  name = hint.to_s.empty? ? "" : "(#{hint})"
  bytes = {
    "Int64"  => 8,
    "Int32"  => 4,
    "UInt32" => 4,
    "Int16"  => 2,
    "Int8"   => 1,
    "Bool"   => 1,
  }[{{type.id}}.to_s] || "?"
  label = {{type.id}}.to_s + "[#{bytes}]#{name}"
  begin
    {% if type.id.stringify == "Bool" %}
      value = io.read_bytes(Int8, IO::ByteFormat::BigEndian)
    {% else %}
      value = io.read_bytes({{type.id}}, IO::ByteFormat::BigEndian) # {{type.id}}
    {% end %}
    if hint.to_s == "error_code" && value != 0
      errmsg = Kafka::Protocol.errmsg(value.to_i16)
      debug "#{label} -> #{errmsg}", color: :red, prefix: {{prefix}}
    else
      debug "#{label} -> #{value}", color: {{color}}, prefix: {{prefix}}
    end

    {% if type.stringify == "Bool" %}
      return (value == 1)
    {% else %}
      return value
    {% end %}
  rescue err
    debug "#{label} (#{err})", color: :red, prefix: {{prefix}}
    raise err
  end
end

macro io_read_bytes_with_debug(color, prefix = nil)
  io_read_bytes_with_debug({{color}}, self, prefix)
end
